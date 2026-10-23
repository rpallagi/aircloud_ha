from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from homeassistant.components.climate import ClimateEntity
from homeassistant.components.climate.const import (
    FAN_AUTO,
    SWING_BOTH,
    SWING_HORIZONTAL,
    SWING_OFF,
    SWING_VERTICAL,
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
)
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.exceptions import HomeAssistantError

from .api import (
    DEFAULT_FAN_SPEED,
    DEFAULT_FAN_SWING,
    DEFAULT_MODE,
    DEFAULT_POWER,
    FAN_SPEED_ORDER,
    NO_HUMIDITY_VALUE,
    _fan_sort_key,
    _get_sys_type,
    build_mode_settings_map,
    get_supported_swing_values,
)
from .const import (
    API,
    CONF_TEMP_ADJUST,
    CONF_TEMP_STEP,
    DOMAIN,
    SYS_TYPE_G_RAC,
)

_LOGGER = logging.getLogger(__name__)

# --- Responsiveness tuning -------------------------------------------------
# Commands are acknowledged to HA immediately (optimistic state) and sent to the
# cloud in the background. While the "trust window" is open, stale cloud
# snapshots are not allowed to overwrite the commanded values; the window closes
# early as soon as the cloud reports the commanded state.
#
# Both values can be overridden at runtime (no restart) with these helpers:
#   input_number.aircloud_trust_window    (seconds, default 30)
#   input_number.aircloud_post_cmd_sleep  (seconds, default 8)
TUNABLE_TRUST_WINDOW = "input_number.aircloud_trust_window"
TUNABLE_POST_CMD_SLEEP = "input_number.aircloud_post_cmd_sleep"
# Fork note: when this input_boolean is "on" the entities advertise only OFF and
# HEAT, so Home Assistant and HomeKit show a simple Off/Heat selector during the
# heating season. Reload the HomeKit bridge after toggling it.
TUNABLE_HEATING_ONLY = "input_boolean.aircloud_heating_only"
DEFAULT_TRUST_WINDOW = 45.0     # measured cloud apply latency: 10-50 s
DEFAULT_POST_CMD_SLEEP = 10.0
COMMAND_DEBOUNCE = 0.7          # coalesce rapid slider / group fan-out calls
MAX_COMMAND_RETRIES = 1         # re-send once if the cloud never confirms
REFRESH_TASKS = "refresh_tasks" # hass.data[DOMAIN][REFRESH_TASKS][family_id]

RAW_TO_HVAC: dict[str, HVACMode] = {
    "COOLING": HVACMode.COOL,
    "HEATING": HVACMode.HEAT,
    "DRY":     HVACMode.DRY,
    "FAN":     HVACMode.FAN_ONLY,
    "AUTO":    HVACMode.AUTO,
}
HVAC_TO_RAW: dict[HVACMode, str] = {v: k for k, v in RAW_TO_HVAC.items()}

RAW_TO_ACTION: dict[str, HVACAction] = {
    "COOLING": HVACAction.COOLING,
    "HEATING": HVACAction.HEATING,
    "DRY":     HVACAction.DRYING,
    "FAN":     HVACAction.FAN,
}

# Fork note: speed names must NOT be the HomeKit-predefined words low/middle/
# medium/high (case-insensitive), otherwise HA's HomeKit bridge adds a fan
# speed slider to the thermostat. With neutral names HA keeps full fan control
# and HomeKit shows no fan speed control at all.
RAW_TO_FAN: dict[str, str] = {
    "AUTO": FAN_AUTO,
    "LV1":  "Silent",
    "LV2":  "Low speed",
    "LV3":  "Medium speed",
    "LV4":  "High speed",
    "LV5":  "Turbo",
    "LV6":  "Turbo+",
    "LVA":  "Low-Auto",
    "NOWIND": "No Wind",
}
FAN_TO_RAW: dict[str, str] = {v: k for k, v in RAW_TO_FAN.items()}

# Fork note: swing names other than "off" must NOT be the HomeKit-predefined words
# on/both/horizontal/vertical, otherwise HA's HomeKit bridge creates a fan service
# (with a useless on/off switch) just to hold the swing toggle.
RAW_TO_SWING: dict[str, str] = {
    "OFF":        SWING_OFF,
    "VERTICAL":   "Vertical swing",
    "HORIZONTAL": "Horizontal swing",
    "BOTH":       "Both swing",
}
SWING_TO_RAW: dict[str, str] = {v: k for k, v in RAW_TO_SWING.items()}

async def _build_entities(hass) -> list["AirCloudClimateEntity"]:
    api = hass.data[DOMAIN][API]
    entities: list[AirCloudClimateEntity] = []
    family_ids = await api.load_family_ids()
    all_devices: list[tuple[dict, int]] = []
    cloud_ids: list[str] = []
    for family_id in family_ids:
        devices = await api.load_climate_data(family_id)
        for device in devices:
            all_devices.append((device, family_id))
            cid = device.get("cloudId")
            if cid:
                cloud_ids.append(cid)
    if cloud_ids:
        await api.load_rac_configuration(cloud_ids)
    for device, family_id in all_devices:
        rac_config = api.get_cached_rac_config(device.get("cloudId"))
        entities.append(AirCloudClimateEntity(api, device, hass, family_id, rac_config))
    return entities

async def async_setup_platform(hass, config, async_add_entities, discovery_info=None):
    entities = await _build_entities(hass)
    if entities:
        async_add_entities(entities, update_before_add=False)

async def async_setup_entry(hass, config_entry, async_add_entities):
    entities = await _build_entities(hass)
    if entities:
        async_add_entities(entities, update_before_add=False)

class AirCloudClimateEntity(ClimateEntity):

    _attr_has_entity_name = True
    _enable_turn_on_off_backwards_compatibility = False

    def __init__(
        self,
        api,
        device: dict[str, Any],
        hass,
        family_id: int,
        rac_config: dict[str, Any] | None,
    ) -> None:
        self._api = api
        self._hass = hass
        self._family_id = family_id
        self._device_id: int = device["id"]
        self._vendor_id: str = device["vendorThingId"]
        self._cloud_id: str | None = device.get("cloudId")
        self._rac_config = rac_config
        self._mode_settings: dict[str, Any] = build_mode_settings_map(rac_config)
        self._supported_raw_swings: list[str] = get_supported_swing_values(rac_config)

        self._attr_unique_id = self._vendor_id
        self._attr_name: str = device["name"]

        self._available = True
        self._power = DEFAULT_POWER
        self._mode = DEFAULT_MODE
        self._fan_speed = DEFAULT_FAN_SPEED
        self._fan_swing = DEFAULT_FAN_SWING
        self._target_temp: float = 24.0
        self._room_temp: float | None = None
        self._raw_humidity: Any = NO_HUMIDITY_VALUE
        self._sys_type: int | None = None
        self._command_in_flight = False

        # optimistic-command bookkeeping
        self._trust_until: float = 0.0
        self._desired: dict[str, Any] | None = None
        self._pending: dict[str, Any] | None = None
        self._dispatch_task: asyncio.Task | None = None
        self._retries = 0
        self._unsub_push = None

        self._apply_snapshot(device)

        self._attr_hvac_modes = self._build_hvac_modes()
        self._attr_fan_modes = self._build_fan_modes()
        self._attr_swing_modes = self._build_swing_modes()

    # ------------------------------------------------------------------ push
    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self._unsub_push = self._api.subscribe(self._family_id, self._on_push)

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_push:
            self._unsub_push()
            self._unsub_push = None
        await super().async_will_remove_from_hass()

    def _on_push(self, devices: list[dict[str, Any]]) -> None:
        """Called by the API (in the event loop) whenever fresh family data arrives."""
        for device in devices:
            if device.get("id") == self._device_id:
                self._apply_snapshot(device)
                if self.hass is not None and self.entity_id:
                    self.async_write_ha_state()
                break

    # --------------------------------------------------------------- helpers
    def _tunable(self, entity_id: str, default: float, lo: float, hi: float) -> float:
        try:
            state = self._hass.states.get(entity_id)
            if state is None or state.state in ("unknown", "unavailable", ""):
                return default
            return max(lo, min(hi, float(state.state)))
        except (TypeError, ValueError, AttributeError):
            return default

    def _build_hvac_modes(self) -> list[HVACMode]:
        modes = [HVACMode.OFF]
        raw_modes = list(self._mode_settings) or [self._mode]
        for raw in RAW_TO_HVAC:
            if raw in raw_modes and RAW_TO_HVAC[raw] not in modes:
                modes.append(RAW_TO_HVAC[raw])
        return modes

    def _build_fan_modes(self) -> list[str]:
        all_speeds: set[str] = set()
        for settings in self._mode_settings.values():
            all_speeds.update(settings.get("enabled_fan_speeds") or [])
        if not all_speeds:
            all_speeds = {self._fan_speed}
        ordered = [
            RAW_TO_FAN[s]
            for s in FAN_SPEED_ORDER
            if s in all_speeds and s in RAW_TO_FAN
        ]
        return ordered or [FAN_AUTO]

    def _build_swing_modes(self) -> list[str]:
        modes = [SWING_OFF]
        for raw in ("VERTICAL", "HORIZONTAL", "BOTH"):
            if raw in self._supported_raw_swings and RAW_TO_SWING.get(raw) not in modes:
                modes.append(RAW_TO_SWING[raw])
        return modes

    def _target_from_snapshot(self, snapshot: dict[str, Any], mode: str) -> float:
        settings = self._mode_settings_for(mode)
        is_relative = settings.get("temperature_setting", "ABSOLUTE") == "RELATIVE"
        ref_temp = float(settings.get("reference_temperature", 0.0))
        if is_relative and snapshot.get("relativeTemperature") not in (None, ""):
            raw_t = float(snapshot.get("relativeTemperature", 0.0))
            return self._clamp_temp(raw_t + ref_temp, mode)
        if snapshot.get("iduTemperature") not in (None, ""):
            raw_t = float(snapshot.get("iduTemperature", 24.0))
            return self._clamp_temp(raw_t, mode)
        return self._default_temp(mode)

    def _snapshot_matches_desired(self, snapshot: dict[str, Any]) -> bool:
        d = self._desired
        if not d:
            return True
        power = str(snapshot.get("power", "")).upper()
        mode = str(snapshot.get("mode", "")).upper()
        if power != d["power"]:
            return False
        if power == "OFF":
            # when off, the cloud may report any mode/fan; only power matters
            return True
        if mode != d["mode"]:
            return False
        if str(snapshot.get("fanSpeed", "")).upper() != d["fan_speed"]:
            return False
        if str(snapshot.get("fanSwing", "")).upper() != d["fan_swing"]:
            return False
        return abs(self._target_from_snapshot(snapshot, mode) - d["target_temp"]) < 0.05

    def _apply_snapshot(self, snapshot: dict[str, Any]) -> None:
        self._available = bool(
            snapshot.get("online", snapshot.get("isOnline", True))
        )
        self._sys_type = _get_sys_type(snapshot)

        trusting = False
        if self._desired is not None:
            if self._snapshot_matches_desired(snapshot):
                _LOGGER.debug(
                    "AirCloud: device=%s cloud confirmed commanded state, closing trust window",
                    self._device_id,
                )
                self._trust_until = 0.0
                self._desired = None
            elif time.monotonic() < self._trust_until:
                trusting = True
                _LOGGER.debug(
                    "AirCloud: device=%s ignoring stale cloud snapshot during trust window "
                    "(cloud power=%s mode=%s temp=%s fan=%s)",
                    self._device_id, snapshot.get("power"), snapshot.get("mode"),
                    snapshot.get("iduTemperature"), snapshot.get("fanSpeed"),
                )
            elif self._retries < MAX_COMMAND_RETRIES and self._pending is None:
                # window expired and the cloud still shows something else:
                # the command was probably dropped -> send it once more
                self._retries += 1
                _LOGGER.warning(
                    "AirCloud: device=%s cloud did not confirm command within trust window "
                    "(cloud power=%s mode=%s temp=%s), re-sending (retry %d/%d)",
                    self._device_id, snapshot.get("power"), snapshot.get("mode"),
                    snapshot.get("iduTemperature"), self._retries, MAX_COMMAND_RETRIES,
                )
                trusting = True
                self._trust_until = time.monotonic() + self._tunable(
                    TUNABLE_TRUST_WINDOW, DEFAULT_TRUST_WINDOW, 5.0, 300.0
                )
                self._pending = self._desired
                if self._dispatch_task is None or self._dispatch_task.done():
                    self._dispatch_task = self._hass.async_create_background_task(
                        self._dispatch_loop(),
                        name=f"air_cloud dispatch {self._device_id}",
                    )
            else:
                _LOGGER.warning(
                    "AirCloud: device=%s giving up on unconfirmed command, showing cloud state "
                    "(cloud power=%s mode=%s temp=%s)",
                    self._device_id, snapshot.get("power"), snapshot.get("mode"),
                    snapshot.get("iduTemperature"),
                )
                self._trust_until = 0.0
                self._desired = None

        if not trusting:
            self._power = str(snapshot.get("power", self._power)).upper()
            self._mode = str(snapshot.get("mode", self._mode)).upper()
            self._fan_speed = str(snapshot.get("fanSpeed", self._fan_speed)).upper()
            self._fan_swing = str(snapshot.get("fanSwing", self._fan_swing)).upper()
            self._target_temp = self._target_from_snapshot(snapshot, self._mode)

        # read-only fields always come from the cloud
        room_raw = snapshot.get("roomTemperature")
        if room_raw is not None:
            adjust = float(
                self._hass.data.get(DOMAIN, {})
                .get(CONF_TEMP_ADJUST, {})
                .get(self._device_id, 0.0)
            )
            self._room_temp = float(room_raw) + adjust
        else:
            self._room_temp = None

        self._raw_humidity = snapshot.get("humidity", NO_HUMIDITY_VALUE)

    def _mode_settings_for(self, mode: str | None = None) -> dict[str, Any]:
        key = str(mode or self._mode or DEFAULT_MODE).upper()
        return self._mode_settings.get(key) or {}

    def _default_temp(self, mode: str) -> float:
        s = self._mode_settings_for(mode)
        ref = float(s.get("reference_temperature", 0.0))
        default = float(s.get("default_temperature", 24.0))
        return default + ref

    def _temp_range_for(self, mode: str | None = None) -> tuple[float, float]:
        s = self._mode_settings_for(mode)
        ref = float(s.get("reference_temperature", 0.0))
        lo = float(s.get("min_temperature", 16.0)) + ref
        hi = float(s.get("max_temperature", 32.0)) + ref
        return lo, hi

    def _union_temp_range(self) -> tuple[float, float]:
        ranges = [
            self._temp_range_for(mode)
            for mode, settings in self._mode_settings.items()
            if settings.get("temperature_enabled", True)
        ]
        if not ranges:
            return 16.0, 32.0
        return min(lo for lo, _ in ranges), max(hi for _, hi in ranges)

    def _reported_temp_range(self) -> tuple[float, float]:
        if not self._mode_settings_for(self._mode).get("temperature_enabled", True):
            return self._union_temp_range()
        return self._temp_range_for(self._mode)

    def _clamp_temp(self, value: float, mode: str) -> float:
        lo, hi = self._temp_range_for(mode)
        if lo > hi:
            return value
        return max(lo, min(hi, value))

    def _normalize_fan_speed(self, raw_speed: str | None, mode: str) -> str:
        s = self._mode_settings_for(mode)
        enabled: list[str] = s.get("enabled_fan_speeds") or []
        candidate = str(raw_speed or self._fan_speed or DEFAULT_FAN_SPEED).upper()
        if not enabled or not s.get("fan_enabled", True):
            return candidate
        if candidate in enabled:
            return candidate
        default = s.get("default_fan_speed", DEFAULT_FAN_SPEED)
        return default if default in enabled else sorted(enabled, key=_fan_sort_key)[0]

    def _normalize_fan_swing(self, raw_swing: str | None) -> str:
        candidate = str(raw_swing or self._fan_swing or DEFAULT_FAN_SWING).upper()
        if candidate == "AUTO" and self._sys_type == SYS_TYPE_G_RAC:
            candidate = DEFAULT_FAN_SWING
        if not self._supported_raw_swings:
            return candidate
        if candidate in self._supported_raw_swings:
            return candidate
        current = str(self._fan_swing or DEFAULT_FAN_SWING).upper()
        if current == "AUTO" and self._sys_type == SYS_TYPE_G_RAC:
            current = DEFAULT_FAN_SWING
        return current if current in self._supported_raw_swings else DEFAULT_FAN_SWING

    def _command_humidity(self, mode: str) -> int:
        s = self._mode_settings_for(mode)
        default = int(s.get("default_humidity", 0))
        if not s.get("humidity_enabled", False):
            return default

        raw = self._raw_humidity
        try:
            val = int(float(raw)) if raw not in (None, "", NO_HUMIDITY_VALUE) else NO_HUMIDITY_VALUE
        except (TypeError, ValueError):
            val = NO_HUMIDITY_VALUE

        if val >= NO_HUMIDITY_VALUE:
            return default

        min_h = s.get("min_humidity")
        max_h = s.get("max_humidity")
        if min_h is not None and max_h is not None and min_h <= max_h:
            if not (min_h <= val <= max_h):
                return default
        return val

    # -------------------------------------------------------------- commands
    async def _send_command(
        self,
        *,
        power: str | None = None,
        mode: str | None = None,
        target_temp: float | None = None,
        fan_speed: str | None = None,
        fan_swing: str | None = None,
    ) -> None:
        """Apply the command optimistically and return immediately.

        The cloud round trip runs in a background task (see _dispatch_loop);
        rapid successive calls are coalesced into a single PUT.
        """
        desired_mode = str(mode or self._mode or DEFAULT_MODE).upper()
        desired_power = (
            "ON" if str(power or self._power or DEFAULT_POWER).upper() == "ON" else "OFF"
        )
        desired_temp = self._clamp_temp(
            target_temp if target_temp is not None else self._target_temp,
            desired_mode,
        )
        if fan_speed is None and desired_power == "ON" and self._power != "ON":
            # Fork note: while OFF the cloud may report any stale fan speed; do not
            # carry it into power-on, start on AUTO (falls back to the mode default
            # where AUTO is not allowed, e.g. DRY/FAN).
            fan_speed = DEFAULT_FAN_SPEED
            _LOGGER.debug(
                "AirCloud: device=%s power-on, resetting fan to %s", self._device_id, fan_speed
            )
        desired_fan = self._normalize_fan_speed(fan_speed, desired_mode)
        desired_swing = self._normalize_fan_swing(fan_swing)
        desired_humidity = self._command_humidity(desired_mode)

        desired = {
            "power": desired_power,
            "mode": desired_mode,
            "target_temp": desired_temp,
            "fan_speed": desired_fan,
            "fan_swing": desired_swing,
            "humidity": desired_humidity,
        }

        # optimistic local state + trust window
        self._power = desired_power
        self._mode = desired_mode
        self._target_temp = desired_temp
        self._fan_speed = desired_fan
        self._fan_swing = desired_swing
        self._desired = desired
        self._retries = 0
        self._trust_until = time.monotonic() + self._tunable(
            TUNABLE_TRUST_WINDOW, DEFAULT_TRUST_WINDOW, 5.0, 300.0
        )
        self.async_write_ha_state()

        # coalesce + dispatch in background
        self._pending = desired
        if self._dispatch_task is None or self._dispatch_task.done():
            self._dispatch_task = self._hass.async_create_background_task(
                self._dispatch_loop(),
                name=f"air_cloud dispatch {self._device_id}",
            )

    async def _dispatch_loop(self) -> None:
        await asyncio.sleep(COMMAND_DEBOUNCE)
        failed = False
        while self._pending is not None:
            cmd = self._pending
            self._pending = None
            self._command_in_flight = True
            started = time.monotonic()
            try:
                await self._api.execute_command(
                    self._device_id,
                    self._family_id,
                    cmd["power"],
                    cmd["target_temp"],
                    cmd["mode"],
                    cmd["fan_speed"],
                    cmd["fan_swing"],
                    cmd["humidity"],
                )
                _LOGGER.info(
                    "AirCloud: command sent device=%s power=%s mode=%s temp=%.1f fan=%s swing=%s (%.1fs)",
                    self._device_id, cmd["power"], cmd["mode"], cmd["target_temp"],
                    cmd["fan_speed"], cmd["fan_swing"], time.monotonic() - started,
                )
                failed = False
            except Exception as exc:  # noqa: BLE001
                failed = True
                _LOGGER.error(
                    "AirCloud: command FAILED device=%s power=%s mode=%s temp=%.1f: %s",
                    self._device_id, cmd["power"], cmd["mode"], cmd["target_temp"], exc,
                )
            finally:
                self._command_in_flight = False

        if failed:
            # stop trusting the optimistic state and show the real one ASAP
            self._trust_until = 0.0
            self._desired = None
            self._schedule_family_refresh(1.0)
        else:
            self._schedule_family_refresh(
                self._tunable(TUNABLE_POST_CMD_SLEEP, DEFAULT_POST_CMD_SLEEP, 2.0, 60.0)
            )

    def _schedule_family_refresh(self, delay: float) -> None:
        """One debounced forced refresh per family, shared by all entities."""
        store = self._hass.data.setdefault(DOMAIN, {}).setdefault(REFRESH_TASKS, {})
        entry = store.get(self._family_id)
        due = time.monotonic() + delay
        if entry and not entry["task"].done():
            entry["due"] = max(entry["due"], due)
            return
        entry = {"due": due, "task": None}
        entry["task"] = self._hass.async_create_background_task(
            self._family_refresh(entry),
            name=f"air_cloud refresh family {self._family_id}",
        )
        store[self._family_id] = entry

    async def _family_refresh(self, entry: dict[str, Any]) -> None:
        while True:
            remaining = entry["due"] - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(remaining)
        try:
            # api notifies every subscribed entity of this family itself
            await self._api.load_climate_data(self._family_id, force=True)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning("AirCloud: post-command refresh failed: %s", exc)

    # ------------------------------------------------------------ properties
    @property
    def unique_id(self) -> str:
        return self._attr_unique_id

    @property
    def name(self) -> str:
        return self._attr_name

    @property
    def device_info(self) -> dict:
        return {
            "identifiers": {(DOMAIN, self._vendor_id)},
            "name": self._attr_name,
            "manufacturer": "Hitachi",
            "model": "AirCloud Climate",
        }

    @property
    def available(self) -> bool:
        return self._available

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {
            "family_id": self._family_id,
            "air_cloud_id": self._device_id,
            "cloud_id": self._cloud_id,
            "sys_type": self._sys_type,
            "command_pending": self._desired is not None
            and time.monotonic() < self._trust_until,
        }

    @property
    def supported_features(self) -> ClimateEntityFeature:
        features = ClimateEntityFeature.TURN_ON | ClimateEntityFeature.TURN_OFF
        if not self._mode_settings or any(
            s.get("temperature_enabled", True) for s in self._mode_settings.values()
        ):
            features |= ClimateEntityFeature.TARGET_TEMPERATURE
        if len(self._attr_fan_modes or []) > 1:
            features |= ClimateEntityFeature.FAN_MODE
        if len(self._attr_swing_modes or []) > 1:
            features |= ClimateEntityFeature.SWING_MODE
        return features

    @property
    def temperature_unit(self) -> str:
        return UnitOfTemperature.CELSIUS

    @property
    def current_temperature(self) -> float | None:
        return self._room_temp

    @property
    def target_temperature(self) -> float:
        return self._target_temp

    @property
    def target_temperature_step(self) -> float:
        return float(
            self._hass.data.get(DOMAIN, {})
            .get(CONF_TEMP_STEP, {})
            .get(self._device_id, 0.5)
        )

    @property
    def min_temp(self) -> float:
        return self._reported_temp_range()[0]

    @property
    def max_temp(self) -> float:
        return self._reported_temp_range()[1]

    @property
    def hvac_mode(self) -> HVACMode:
        if self._power == "OFF":
            return HVACMode.OFF
        return RAW_TO_HVAC.get(self._mode, HVACMode.OFF)

    @property
    def hvac_action(self) -> HVACAction | None:
        if self._power == "OFF":
            return HVACAction.OFF
        if self._mode in RAW_TO_ACTION:
            return RAW_TO_ACTION[self._mode]
        if self._mode == "AUTO":
            if self._room_temp is None:
                return None
            return (
                HVACAction.COOLING
                if self._room_temp > self._target_temp
                else HVACAction.HEATING
            )
        return None

    @property
    def hvac_modes(self) -> list[HVACMode]:
        if self._heating_only():
            return [m for m in self._attr_hvac_modes if m in (HVACMode.OFF, HVACMode.HEAT)]
        return self._attr_hvac_modes

    def _heating_only(self) -> bool:
        try:
            state = self._hass.states.get(TUNABLE_HEATING_ONLY)
        except AttributeError:
            return False
        return state is not None and state.state == "on"

    @property
    def fan_mode(self) -> str:
        return RAW_TO_FAN.get(self._fan_speed, FAN_AUTO)

    @property
    def fan_modes(self) -> list[str]:
        return self._attr_fan_modes

    @property
    def swing_mode(self) -> str:
        return RAW_TO_SWING.get(self._fan_swing, SWING_OFF)

    @property
    def swing_modes(self) -> list[str]:
        return self._attr_swing_modes

    async def async_turn_on(self) -> None:
        await self._send_command(power="ON")

    async def async_turn_off(self) -> None:
        await self._send_command(power="OFF")

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        if hvac_mode == HVACMode.OFF:
            await self._send_command(power="OFF")
            return
        raw_mode = HVAC_TO_RAW.get(hvac_mode, self._mode)
        mode_changed = raw_mode != self._mode
        await self._send_command(
            power="ON",
            mode=raw_mode,
            target_temp=self._default_temp(raw_mode) if mode_changed else None,
            fan_speed=(
                self._mode_settings_for(raw_mode).get("default_fan_speed")
                if mode_changed else None
            ),
        )

    async def async_set_fan_mode(self, fan_mode: str) -> None:
        raw = FAN_TO_RAW.get(fan_mode, DEFAULT_FAN_SPEED)
        await self._send_command(fan_speed=raw)

    async def async_set_swing_mode(self, swing_mode: str) -> None:
        raw = SWING_TO_RAW.get(swing_mode, DEFAULT_FAN_SWING)
        await self._send_command(fan_swing=raw)

    async def async_set_temperature(self, **kwargs: Any) -> None:
        temp = kwargs.get(ATTR_TEMPERATURE)
        if temp is None:
            return
        hvac_mode = kwargs.get("hvac_mode")
        if hvac_mode == HVACMode.OFF:
            await self._send_command(power="OFF", target_temp=float(temp))
            return
        desired_mode = (
            HVAC_TO_RAW.get(hvac_mode, self._mode)
            if hvac_mode is not None
            else self._mode
        )
        desired_power = "ON" if hvac_mode is not None and hvac_mode != HVACMode.OFF else self._power
        await self._send_command(power=desired_power, mode=desired_mode, target_temp=float(temp))

    async def async_update(self) -> None:
        if self._command_in_flight:
            return
        devices = await self._api.load_climate_data(self._family_id)
        for device in devices:
            if device.get("id") == self._device_id:
                self._apply_snapshot(device)
                break
