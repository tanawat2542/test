from enum import Enum, auto
import logging

_log = logging.getLogger(__name__)


class OAUState(Enum):
    ON = "on"
    OFF = "off"
    DEFAULT = "off"


class DeviceStore:
    def __init__(self, id: str):
        self.id = id
        self._data = {}
    
    @property
    def data(self):
        return self._data
        
    def update_data(self, message):
        self._data = message
        

class ZoneStore:
    def __init__(self, name: str):
        self.name = name
        self.oau_device_ids = []
        self.device_instances = {}
        self.OAU_status = OAUState.DEFAULT  # Initialize OAU status as OFF
        
    def add_device(self, device_id: str, device_instance: object):
        self.device_instances[device_id] = device_instance
        
    def execute_automation(self, CO2_on=1000, CO2_off=800):

        # check if all device's CO2 levels are below the threshold to turn it OFF
        if all(device.data.get('co2', 0) < CO2_off for device in self.device_instances.values()):
            _log.info(f"All CO2 levels are below threshold. Turning OAU OFF for zone `{self.name}`")
            self.OAU_status = OAUState.OFF
            return True, OAUState.OFF
        
        # Check if any device's CO2 level exceeds the threshold to turn OAU ON
        for device in self.device_instances.values():
            if device.data.get('co2', 0) > CO2_on:
                _log.info(f"CO2 level exceeds threshold. Turning OAU ON for zone `{self.name}`")
                self.OAU_status = OAUState.ON
                return True, OAUState.ON
        
        self.OAU_status = OAUState.DEFAULT
        return False, OAUState.DEFAULT
        
    def set_oau_device_ids(self, oau_device_ids: list):
        self.oau_device_ids = oau_device_ids
            