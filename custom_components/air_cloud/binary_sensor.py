from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)

from .const import API, DOMAIN

_LOGGER = logging.getLogger(__name__)

class AirCloudFrostWashBinarySensor(BinarySensorEntity):

    _attr_has_entity_name = True
    _attr_device_class = BinarySensorDeviceClass.RUNNING
    # Updated from the shared family snapshot (push listener / climate polls);
    # no independent polling, so it never competes for the API lock.
    _attr_should_poll = False

    def __init__(self, api, device: dict[str, Any], family_id: int) -> None:
        self._api = api
        self._device_id = device["id"]
        self._name = device["name"]
        self._vendor_id = device["vendorThingId"]
        self._family_id = family_id
        self._unsub_push = None

        self._attr_unique_id = f"{self._vendor_id}_frost_wash_status"
        self._attr_name = "FrostWash Running"

        self._attr_is_on = self._check_is_on(device)

    @property
    def device_info(self) -> dict:
        return {
            "identifiers": {(DOMAIN, self._vendor_id)},
            "name": self._name,
            "manufacturer": "Hitachi",
            "model": "AirCloud Climate",
        }

    def _check_is_on(self, device: dict[str, Any]) -> bool:
        status = device.get("iduFrostWashStatus")
        if isinstance(status, dict):
            return bool(status.get("active", False))
        return False

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self._unsub_push = self._api.subscribe(self._family_id, self._on_push)

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_push:
            self._unsub_push()
            self._unsub_push = None
        await super().async_will_remove_from_hass()

    def _on_push(self, devices: list[dict[str, Any]]) -> None:
        for device in devices:
            if device.get("id") == self._device_id:
                new = self._check_is_on(device)
                if new != self._attr_is_on:
                    self._attr_is_on = new
                    if self.hass is not None and self.entity_id:
                        self.async_write_ha_state()
                break

    async def async_update(self) -> None:
        devices = await self._api.load_climate_data(self._family_id)
        for device in devices:
            if device.get("id") == self._device_id:
                self._attr_is_on = self._check_is_on(device)
                break

async def async_setup_entry(hass, config_entry, async_add_entities) -> None:
    api = hass.data[DOMAIN][API]
    entities: list[BinarySensorEntity] = []

    if api._device_cache:
        device_list = [
            (device, family_id)
            for device_id, device in api._device_cache.items()
            for family_id in [api._device_family.get(device_id)]
            if family_id is not None
        ]
    else:
        device_list = []
        for family_id in await api.load_family_ids():
            for device in await api.load_climate_data(family_id):
                device_list.append((device, family_id))

    for device, family_id in device_list:
        # Check if FrostWash is supported by the device
        if device.get("iduFrostWash", False) or "iduFrostWashStatus" in device:
            entities.append(AirCloudFrostWashBinarySensor(api, device, family_id))

    if entities:
        async_add_entities(entities)
