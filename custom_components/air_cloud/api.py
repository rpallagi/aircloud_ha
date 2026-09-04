from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

import aiohttp
from aiohttp import WSMsgType

from .const import (
    HOST_API,
    SYS_TYPE_G_PAC,
    SYS_TYPE_G_RAC,
    SYS_TYPE_YUTAMPO,
    URN_AUTH,
    URN_CONTROL_PAC_STATUS,
    URN_CONTROL_STATUS,
    URN_CONTROL_YUTAMPO_STATUS,
    URN_ENERGY_CONSUMPTION_SUMMARY,
    URN_FROST_WASH,
    URN_RAC_CONFIGURATION,
    URN_REFRESH_TOKEN,
    URN_WHO,
    URN_WSS,
    USER_AGENT,
)

_LOGGER = logging.getLogger(__name__)

NO_HUMIDITY_VALUE: int = 2_147_483_647
ENERGY_SUMMARY_CACHE_SECONDS: float = 3600.0
ENERGY_SUMMARY_RATE_LIMIT_SECONDS: float = 1800.0

# REST / WS timing
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
COMMAND_TIMEOUT = aiohttp.ClientTimeout(total=15)
WS_CONNECT_TIMEOUT = aiohttp.ClientTimeout(total=60)
CACHE_TTL_POLLING: float = 15.0     # one-shot reads while no push listener is up
CACHE_TTL_PUSH: float = 60.0        # re-read interval while push is healthy (cloud pushes are unreliable)
LISTENER_HEARTBEAT: float = 8.0     # STOMP client heart-beat (server asked for 10 s)
LISTENER_IDLE_TIMEOUT: float = 45.0 # no frame (not even a heart-beat) -> reconnect
LISTENER_BACKOFF_MIN: float = 5.0
LISTENER_BACKOFF_MAX: float = 60.0

DEFAULT_POWER = "OFF"
DEFAULT_MODE = "COOLING"
DEFAULT_FAN_SPEED = "AUTO"
DEFAULT_FAN_SWING = "OFF"

FAN_SPEED_ORDER: tuple[str, ...] = (
    "AUTO",
    "LV1",
    "LV2",
    "LV3",
    "LV4",
    "LV5",
    "LV6",
    "LVA",
    "NOWIND",
)

def _fan_sort_key(speed: str) -> int:
    try:
        return FAN_SPEED_ORDER.index(speed.upper())
    except ValueError:
        return len(FAN_SPEED_ORDER)

class AirCloudApi:
    def __init__(self, login: str, password: str) -> None:
        self._login = login
        self._password = password
        self._token: str | None = None
        self._ref_token: str | None = None
        self._last_token_update: datetime = datetime.now()
        self._device_cache: dict[int, dict[str, Any]] = {}
        self._device_family: dict[int, int] = {}
        self._rac_config_cache: dict[str, dict[str, Any]] = {}
        self._energy_summary_cache: dict[int, dict[str, Any]] = {}
        self._energy_summary_timestamps: dict[int, float] = {}
        self._energy_summary_rate_limited_until: dict[int, float] = {}
        self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)

        self._update_locks: dict[int, asyncio.Lock] = {}
        self._update_timestamps: dict[int, float] = {}
        self._energy_summary_locks: dict[int, asyncio.Lock] = {}

        # persistent push listener (one per family)
        self._subscribers: dict[int, list[Callable[[list[dict[str, Any]]], None]]] = {}
        self._listener_tasks: dict[int, asyncio.Task] = {}
        self._listener_connected: dict[int, bool] = {}
        self._listener_messages: dict[int, int] = {}

    # ------------------------------------------------------------- push API
    def subscribe(
        self,
        family_id: int,
        callback: Callable[[list[dict[str, Any]]], None],
    ) -> Callable[[], None]:
        """Register a callback for fresh device data of a family.

        Starts the persistent STOMP listener for that family on first use.
        Returns an unsubscribe function.
        """
        self._subscribers.setdefault(family_id, []).append(callback)
        task = self._listener_tasks.get(family_id)
        if task is None or task.done():
            self._listener_tasks[family_id] = asyncio.get_running_loop().create_task(
                self._listener_loop(family_id),
                name=f"air_cloud listener {family_id}",
            )

        def _unsub() -> None:
            subs = self._subscribers.get(family_id, [])
            if callback in subs:
                subs.remove(callback)

        return _unsub

    def listener_healthy(self, family_id: int) -> bool:
        return self._listener_connected.get(family_id, False)

    def _notify(self, family_id: int, devices: list[dict[str, Any]]) -> None:
        for cb in list(self._subscribers.get(family_id, [])):
            try:
                cb(devices)
            except Exception as exc:  # noqa: BLE001
                _LOGGER.warning("AirCloud: subscriber callback failed: %s", exc)

    def _parse_ws_message(self, raw: str) -> list[dict[str, Any]] | None:
        json_part = "{" + raw.partition("{")[2].replace("\0", "")
        try:
            struct = json.loads(json_part)
        except json.JSONDecodeError as exc:
            _LOGGER.warning("AirCloud: failed to parse WS JSON: %s", exc)
            return None
        return struct.get("data") or []

    def _store_devices(self, family_id: int, devices: list[dict[str, Any]]) -> None:
        for device in devices:
            dev_id = device.get("id")
            if dev_id is not None:
                self._device_cache[dev_id] = device
                self._device_family[dev_id] = family_id
        self._update_timestamps[family_id] = asyncio.get_running_loop().time()

    async def _listener_loop(self, family_id: int) -> None:
        backoff = LISTENER_BACKOFF_MIN
        while not self._session.closed:
            hb_task: asyncio.Task | None = None
            try:
                await self._refresh_token()
                async with self._session.ws_connect(
                    URN_WSS,
                    timeout=WS_CONNECT_TIMEOUT,
                    headers={"User-Agent": USER_AGENT},
                ) as ws:
                    await ws.send_str(self._build_stomp_connect(family_id))
                    hb_task = asyncio.get_running_loop().create_task(self._heartbeat(ws))
                    while True:
                        msg = await asyncio.wait_for(ws.receive(), timeout=LISTENER_IDLE_TIMEOUT)
                        if msg.type == WSMsgType.TEXT:
                            data = msg.data
                            if data.startswith("CONNECTED"):
                                if "user-name:" not in data:
                                    _LOGGER.warning("AirCloud: push listener auth rejected, re-authenticating")
                                    await self._refresh_token(forced=True)
                                    break
                                self._listener_connected[family_id] = True
                                backoff = LISTENER_BACKOFF_MIN
                                _LOGGER.info("AirCloud: push listener connected (family %s)", family_id)
                            elif data.startswith("MESSAGE") and "{" in data:
                                devices = self._parse_ws_message(data)
                                if devices:
                                    self._store_devices(family_id, devices)
                                    self._listener_messages[family_id] = (
                                        self._listener_messages.get(family_id, 0) + 1
                                    )
                                    _LOGGER.debug(
                                        "AirCloud: push #%d for family %s (%d devices)",
                                        self._listener_messages[family_id], family_id, len(devices),
                                    )
                                    self._notify(family_id, devices)
                            elif data.startswith("ERROR"):
                                _LOGGER.warning("AirCloud: push listener STOMP error: %s", data[:200])
                                break
                            # heart-beats ("\n") and RECEIPT frames are ignored
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSING, WSMsgType.ERROR):
                            _LOGGER.debug("AirCloud: push listener socket closed (%s)", msg.type)
                            break
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                _LOGGER.debug("AirCloud: push listener idle timeout, reconnecting")
            except Exception as exc:  # noqa: BLE001
                _LOGGER.warning("AirCloud: push listener error: %s", exc)
            finally:
                self._listener_connected[family_id] = False
                if hb_task:
                    hb_task.cancel()
            if self._session.closed:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, LISTENER_BACKOFF_MAX)

    async def _heartbeat(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            while not ws.closed:
                await asyncio.sleep(LISTENER_HEARTBEAT)
                await ws.send_str("\n")
        except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
            pass
        except Exception as exc:  # noqa: BLE001
            _LOGGER.debug("AirCloud: heartbeat stopped: %s", exc)

    def _get_update_lock(self, family_id: int) -> asyncio.Lock:
        if family_id not in self._update_locks:
            self._update_locks[family_id] = asyncio.Lock()
        return self._update_locks[family_id]

    def _get_energy_summary_lock(self, family_id: int) -> asyncio.Lock:
        if family_id not in self._energy_summary_locks:
            self._energy_summary_locks[family_id] = asyncio.Lock()
        return self._energy_summary_locks[family_id]

    async def validate_credentials(self) -> bool:
        try:
            await self._authenticate()
            return True
        except Exception as exc: 
            _LOGGER.error("AirCloud: failed to validate credentials: %s", exc)
            return False

    async def _authenticate(self) -> None:
        async with self._session.post(
            HOST_API + URN_AUTH,
            json={"email": self._login, "password": self._password},
            headers={"User-Agent": USER_AGENT},
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        self._token = data["token"]
        self._ref_token = data["refreshToken"]
        self._last_token_update = datetime.now()
        _LOGGER.debug("AirCloud: authenticated, token obtained")

    async def _refresh_token(self, *, forced: bool = False) -> None:
        if self._token is None or forced:
            await self._authenticate()
            return

        age = (datetime.now() - self._last_token_update).total_seconds()
        if age <= 300: 
            return

        _LOGGER.debug("AirCloud: token older than 5 min, refreshing")
        async with self._session.post(
            HOST_API + URN_REFRESH_TOKEN,
            headers={
                "Authorization": f"Bearer {self._ref_token}",
                "isRefreshToken": "true",
                "User-Agent": USER_AGENT,
            },
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        self._token = data["token"]
        self._ref_token = data["refreshToken"]
        self._last_token_update = datetime.now()
        _LOGGER.debug("AirCloud: token refreshed")

    async def load_family_ids(self) -> list[int]:
        await self._refresh_token()
        async with self._session.get(
            HOST_API + URN_WHO,
            headers=self._headers(),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        return [item["familyId"] for item in data]

    def _cache_is_fresh(self, family_id: int, force: bool) -> bool:
        now = asyncio.get_running_loop().time()
        age = now - self._update_timestamps.get(family_id, 0.0)
        if self.listener_healthy(family_id):
            # push keeps the cache current; a forced read right after a command
            # is redundant if a push already arrived in the last few seconds
            return age < (CACHE_TTL_POLLING if force else CACHE_TTL_PUSH)
        return (not force) and age < CACHE_TTL_POLLING

    async def load_climate_data(self, family_id: int, force: bool = False) -> list[dict[str, Any]]:
        if self._cache_is_fresh(family_id, force):
            return self._get_cached_family_devices(family_id)

        lock = self._get_update_lock(family_id)
        async with lock:
            if self._cache_is_fresh(family_id, force):
                return self._get_cached_family_devices(family_id)

            if self._session.closed:
                return []

            await self._refresh_token()

            try:
                async with self._session.ws_connect(
                    URN_WSS,
                    timeout=WS_CONNECT_TIMEOUT,
                    headers={"User-Agent": USER_AGENT},
                ) as ws:
                    await ws.send_str(self._build_stomp_connect(family_id))
                    raw = await self._recv_stomp_message(ws, family_id)
            except Exception as exc:
                _LOGGER.warning("AirCloud: WebSocket error: %s", exc)
                return self._get_cached_family_devices(family_id)

            if not raw:
                return self._get_cached_family_devices(family_id)

            _LOGGER.debug("AirCloud: received WS data for family %s", family_id)
            devices = self._parse_ws_message(raw)
            if devices is None:
                return self._get_cached_family_devices(family_id)

            self._store_devices(family_id, devices)

        # outside the lock: fan the fresh snapshot out to every entity
        self._notify(family_id, devices)
        return devices

    def _get_cached_family_devices(self, family_id: int) -> list[dict[str, Any]]:
        return [
            dev for dev_id, dev in self._device_cache.items()
            if self._device_family.get(dev_id) == family_id
        ]

    def _build_stomp_connect(self, family_id: int) -> str:
        return (
            "CONNECT\naccept-version:1.1,1.2\nheart-beat:10000,10000\n"
            "Authorization:Bearer {token}\n\n\0\n"
            "SUBSCRIBE\nid:{sub_id}\ndestination:/notification/{fid}/{fid}\nack:auto\n\n\0"
        ).format(token=self._token, sub_id=uuid.uuid4(), fid=family_id)

    async def _recv_stomp_message(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        family_id: int,
        *,
        depth: int = 0,
    ) -> str | None:
        for _ in range(10):
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=10)
            except asyncio.TimeoutError:
                return None

            if msg.type == WSMsgType.TEXT:
                if msg.data.startswith("CONNECTED") and "user-name:" not in msg.data:
                    if depth >= 1:
                        _LOGGER.error("AirCloud: re-auth loop detected, giving up")
                        return None
                    _LOGGER.warning("AirCloud: WS auth failed, re-authenticating")
                    await ws.close()
                    await self._refresh_token(forced=True)
                    async with self._session.ws_connect(
                        URN_WSS,
                        timeout=WS_CONNECT_TIMEOUT,
                        headers={"User-Agent": USER_AGENT},
                    ) as new_ws:
                        await new_ws.send_str(self._build_stomp_connect(family_id))
                        return await self._recv_stomp_message(new_ws, family_id, depth=depth + 1)

                if msg.data.startswith("MESSAGE") and "{" in msg.data:
                    return msg.data

            elif msg.type == WSMsgType.CLOSED:
                return None

        return None

    async def load_energy_consumption_summary(
        self,
        family_id: int,
        force: bool = False,
    ) -> dict[str, Any]:
        now = asyncio.get_running_loop().time()
        cached = self._energy_summary_cache.get(family_id)
        last_update = self._energy_summary_timestamps.get(family_id, 0.0)
        rate_limited_until = self._energy_summary_rate_limited_until.get(family_id, 0.0)

        if cached and not force:
            if now < rate_limited_until or (now - last_update) < ENERGY_SUMMARY_CACHE_SECONDS:
                return cached

        lock = self._get_energy_summary_lock(family_id)
        async with lock:
            now = asyncio.get_running_loop().time()
            cached = self._energy_summary_cache.get(family_id)
            last_update = self._energy_summary_timestamps.get(family_id, 0.0)
            rate_limited_until = self._energy_summary_rate_limited_until.get(family_id, 0.0)

            if cached and not force:
                if now < rate_limited_until or (now - last_update) < ENERGY_SUMMARY_CACHE_SECONDS:
                    return cached

            if self._session.closed:
                return cached or {}

            await self._refresh_token()

            try:
                async with self._session.post(
                    f"{HOST_API}{URN_ENERGY_CONSUMPTION_SUMMARY}?familyId={family_id}",
                    headers=self._headers(),
                    json={"from": "2000-01-01", "to": "2099-12-31"},
                ) as resp:
                    if resp.status == 429:
                        retry_after = _retry_after_seconds(resp.headers.get("Retry-After"))
                        self._energy_summary_rate_limited_until[family_id] = (
                            asyncio.get_running_loop().time() + retry_after
                        )
                        _LOGGER.warning(
                            "AirCloud: energy summary rate limited for family %s, retrying in %.0f seconds",
                            family_id,
                            retry_after,
                        )
                        return cached or {}

                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                _LOGGER.warning(
                    "AirCloud: failed to load energy summary for family %s: %s",
                    family_id,
                    exc,
                )
                return cached or {}

            self._energy_summary_cache[family_id] = data
            self._energy_summary_timestamps[family_id] = asyncio.get_running_loop().time()
            self._energy_summary_rate_limited_until.pop(family_id, None)
            return data

    async def load_rac_configuration(self, cloud_ids: list[str]) -> list[dict[str, Any]]:
        unique = list(dict.fromkeys(cid for cid in cloud_ids if cid))
        if not unique:
            return []
        await self._refresh_token()
        async with self._session.post(
            HOST_API + URN_RAC_CONFIGURATION,
            headers=self._headers(),
            json=unique,
        ) as resp:
            resp.raise_for_status()
            configs: list[dict[str, Any]] = await resp.json(content_type=None)
        for cfg in configs or []:
            cid = cfg.get("cloudId")
            if cid:
                self._rac_config_cache[cid] = cfg
        return configs or []

    def get_cached_rac_config(self, cloud_id: str | None) -> dict[str, Any] | None:
        return self._rac_config_cache.get(cloud_id) if cloud_id else None

    async def execute_command(
        self,
        device_id: int,
        family_id: int,
        power: str,
        temperature: float,
        mode: str,
        fan_speed: str,
        fan_swing: str,
        humidity: int,
    ) -> None:
        if self._session.closed:
            return

        await self._refresh_token()

        snapshot = self._device_cache.get(device_id) or {}
        sys_type = _get_sys_type(snapshot)

        if sys_type == SYS_TYPE_G_PAC:
            await self._execute_gpac_command(device_id, family_id, snapshot, power, temperature, mode, fan_speed)
        elif sys_type == SYS_TYPE_YUTAMPO:
            await self._execute_yutampo_command(device_id, family_id, snapshot, power, temperature, mode)
        else:
            await self._execute_rac_command(device_id, family_id, snapshot, power, temperature, mode, fan_speed, fan_swing, humidity)

    async def _execute_rac_command(
        self,
        device_id: int,
        family_id: int,
        snapshot: dict[str, Any],
        power: str,
        temperature: float,
        mode: str,
        fan_speed: str,
        fan_swing: str,
        humidity: int,
    ) -> None:
        humidity_str = str(humidity) if humidity < NO_HUMIDITY_VALUE else "0"

        payload: dict[str, Any] = {
            "power": power,
            "mode": mode,
            "fanSpeed": fan_speed,
            "fanSwing": fan_swing,
            "humidity": humidity_str,
            "iduTemperature": temperature,
            "relativeTemperature": _compute_relative_temperature(temperature, mode, snapshot),
        }

        url = f"{HOST_API}{URN_CONTROL_STATUS}/{device_id}?familyId={family_id}"
        await self._put(url, payload)
        self._device_cache[device_id] = {**snapshot, **payload, "id": device_id}

    async def _execute_gpac_command(
        self,
        device_id: int,
        family_id: int,
        snapshot: dict[str, Any],
        power: str,
        temperature: float,
        mode: str,
        fan_speed: str,
    ) -> None:
        payload: dict[str, Any] = {
            "id": device_id,
            "buzz": False,
            "power": power,
            "mode": mode,
            "fanSpeed": fan_speed,
            "iduTemperature": temperature,
            "sysType": _get_sys_type(snapshot) or SYS_TYPE_G_PAC,
            "vendorThingId": snapshot.get("vendorThingId", ""),
            "iduIndexPosition": snapshot.get("iduIndexPosition", snapshot.get("indexPosition", 0)),
            "ENFS": snapshot.get("enableFanSpeed", snapshot.get("ENFS", 0)),
            "fanD": snapshot.get("fanDirection", snapshot.get("FanD", 0)),
            "setTAutoC": snapshot.get("setTempDuelCool", snapshot.get("SetTAutoC", temperature)),
            "setTAutoH": snapshot.get("setTempDuelHeat", snapshot.get("SetTAutoH", temperature)),
        }
        url = f"{HOST_API}{URN_CONTROL_PAC_STATUS}/{device_id}?familyId={family_id}"
        await self._put(url, payload)
        self._device_cache[device_id] = {**snapshot, **payload}

    async def _execute_yutampo_command(
        self,
        device_id: int,
        family_id: int,
        snapshot: dict[str, Any],
        power: str,
        temperature: float,
        mode: str,
    ) -> None:
        payload: dict[str, Any] = {
            "id": device_id,
            "sysType": _get_sys_type(snapshot) or SYS_TYPE_YUTAMPO,
            "power": power,
            "mode": mode,
            "iduTemperature": int(round(temperature)),
            "boost": snapshot.get("boost", "OFF"),
            "boostTemperature": snapshot.get("boostTemperature", int(round(temperature))),
        }
        url = f"{HOST_API}{URN_CONTROL_YUTAMPO_STATUS}/{device_id}?familyId={family_id}"
        await self._put(url, payload)
        self._device_cache[device_id] = {**snapshot, **payload}

    async def execute_frost_wash(self, device_id: int, family_id: int) -> None:
        if self._session.closed:
            return

        await self._refresh_token()
        snapshot = self._device_cache.get(device_id) or {}
        
        # The Android app sends the entire DetailedIduModel snapshot as payload
        payload: dict[str, Any] = {**snapshot}
        # The endpoint requires familyId in the query string
        url = f"{HOST_API}{URN_FROST_WASH}/{device_id}?familyId={family_id}"
        
        await self._put(url, payload)

    async def _put(self, url: str, payload: dict[str, Any]) -> None:
        """PUT with a bounded timeout and one retry.

        401 -> re-authenticate and retry; timeouts / 5xx / connection errors ->
        retry once after a short pause. 4xx other than 401 is not retried.
        """
        _LOGGER.debug("AirCloud: PUT %s  payload=%s", url, payload)
        last_exc: Exception | None = None
        for attempt in (1, 2):
            try:
                async with self._session.put(
                    url, headers=self._headers(), json=payload, timeout=COMMAND_TIMEOUT
                ) as resp:
                    body = await resp.text()
                    _LOGGER.debug("AirCloud: response status=%s body=%s", resp.status, body[:500])
                    if resp.status == 401 and attempt == 1:
                        _LOGGER.warning("AirCloud: PUT got 401, re-authenticating and retrying")
                        await self._refresh_token(forced=True)
                        continue
                    if resp.status >= 400:
                        _LOGGER.error(
                            "AirCloud: command failed status=%s url=%s payload=%s body=%s",
                            resp.status, url, payload, body,
                        )
                    if resp.status >= 500 and attempt == 1:
                        last_exc = aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=resp.status, message=body[:200]
                        )
                        await asyncio.sleep(1.5)
                        continue
                    resp.raise_for_status()
                    return
            except (asyncio.TimeoutError, aiohttp.ClientConnectionError) as exc:
                last_exc = exc
                if attempt == 1:
                    _LOGGER.warning("AirCloud: PUT transient error (%s), retrying once", exc)
                    await asyncio.sleep(1.5)
                    continue
                raise
        if last_exc:
            raise last_exc

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def close_session(self) -> None:
        for task in self._listener_tasks.values():
            task.cancel()
        self._listener_tasks.clear()
        self._listener_connected.clear()
        await self._session.close()

def _get_sys_type(snapshot: dict[str, Any]) -> int | None:
    val = snapshot.get("SysType", snapshot.get("sysType"))
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

def _retry_after_seconds(value: str | None) -> float:
    if value is None:
        return ENERGY_SUMMARY_RATE_LIMIT_SECONDS
    try:
        return max(float(value), ENERGY_SUMMARY_CACHE_SECONDS)
    except ValueError:
        return ENERGY_SUMMARY_RATE_LIMIT_SECONDS

def _compute_relative_temperature(
    absolute_temperature: float,
    mode: str,
    snapshot: dict[str, Any],
) -> float:
    if mode == "AUTO":
        rel = snapshot.get("relativeTemperature", 0)
        try:
            return float(rel)
        except (TypeError, ValueError):
            return 0.0
    return 0.0

def build_mode_settings_map(rac_config: dict[str, Any] | None) -> dict[str, Any]:
    if not rac_config:
        return {}
    result: dict[str, Any] = {}
    for mode_data in rac_config.get("racOperationModes", []):
        mode = str(mode_data.get("mode") or "").upper()
        if not mode:
            continue
        enable_settings = mode_data.get("enableSettings") or {}
        enabled_fan_speeds = [
            str(k).upper()
            for k, v in sorted(
                (mode_data.get("enableFanSpeed") or {}).items(),
                key=lambda item: _fan_sort_key(item[0]),
            )
            if v
        ]
        default_fan_candidates = [
            str(k).upper()
            for k, v in sorted(
                (mode_data.get("defaultFanSpeed") or {}).items(),
                key=lambda item: _fan_sort_key(item[0]),
            )
            if v
        ]
        result[mode] = {
            "mode": mode,
            "temperature_setting": str(mode_data.get("temperatureSetting", "ABSOLUTE")).upper(),
            "min_temperature": float(mode_data.get("minTemperature", 16.0)),
            "max_temperature": float(mode_data.get("maxTemperature", 32.0)),
            "default_temperature": float(mode_data.get("defaultTemperature", 24.0)),
            "reference_temperature": float(mode_data.get("referenceTemperature", 0.0)),
            "min_humidity": (float(mode_data["minHumidity"]) if mode_data.get("minHumidity") is not None else None),
            "max_humidity": (float(mode_data["maxHumidity"]) if mode_data.get("maxHumidity") is not None else None),
            "default_humidity": int(float(mode_data.get("defaultHumidity", 0))),
            "temperature_enabled": bool(enable_settings.get("temperature", True)),
            "humidity_enabled": bool(enable_settings.get("humidity", False)),
            "fan_enabled": bool(enable_settings.get("fan", True)),
            "swing_enabled": bool(enable_settings.get("swing", True)),
            "enabled_fan_speeds": enabled_fan_speeds,
            "default_fan_speed": (
                default_fan_candidates[0] if default_fan_candidates
                else enabled_fan_speeds[0] if enabled_fan_speeds
                else DEFAULT_FAN_SPEED
            ),
        }
    return result

def get_supported_swing_values(rac_config: dict[str, Any] | None) -> list[str]:
    supported = ["OFF"]
    if not rac_config:
        return supported
    swing = rac_config.get("swing") or {}
    if swing.get("VERTICAL"):
        supported.append("VERTICAL")
    if swing.get("HORIZONTAL"):
        supported.append("HORIZONTAL")
    if swing.get("VERTICAL") and swing.get("HORIZONTAL"):
        supported.append("BOTH")
    return supported
