""" OAUAgent
Automated OAU controls based on aPMV (adaptive Predicted Mean Vote) and Tenant Feedback
The agent will consider every X minutes and will apply OAU controls to each zone defined in config file

Other agent integration:
1. `subiot` agent: to receive Tenant Feedback from LINE chatbot
2. `mqtt_logger` agent: to send control commands to Niagara via MQTTBroker

To start the agent, run the following commands:
```
vctl remove --tag oau_control
python ~/volttron/scripts/install-agent.py -s ~/alto_os/Agents/Services/OAUAgent -t oau_control -i oau_control
vctl config store oau_control config ~/alto_os/Agents/Services/OAUAgent/config
vctl enable --tag oau_control
vctl start --tag oau_control
```
"""

__docformat__ = 'reStructuredText'

import logging
import sys
import json
import pendulum
from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core, RPC
from volttron.platform.scheduling import periodic, cron

from .datastore import DeviceStore, ZoneStore, OAUState

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"


def oauagent(config_path, **kwargs):
    """
    Parses the Agent configuration and returns an instance of
    the agent created using that configuration.

    :param config_path: Path to a configuration file.
    :type config_path: str
    :returns: Oauagent
    :rtype: Oauagent
    """
    try:
        config = utils.load_config(config_path)
    except Exception:
        config = {}

    if not config:
        _log.info("Using Agent defaults for starting configuration.")

    automation = config.get("automation", dict())
    thermal_zone_mapping = config.get("thermal_zone_mapping", dict())
    cratedb_config = config.get("cratedb_config", dict())

    return Oauagent(automation=automation,
                    thermal_zone_mapping=thermal_zone_mapping, 
                    cratedb_config=cratedb_config, 
                    **kwargs)


class Oauagent(Agent):
    """
    Document agent constructor here.
    """

    def __init__(self, automation=dict(), thermal_zone_mapping=dict(), cratedb_config=dict(), **kwargs):
        super(Oauagent, self).__init__(**kwargs)
        _log.debug("vip_identity: " + self.core.identity)

        # TODO: handle when missing parameter in Database config
        self.cratedb_config = cratedb_config

        self.automation = automation
        self.thermal_zone_mapping = thermal_zone_mapping

        self.CO2_on = self.automation.get("CO2_on", 1000)
        self.CO2_off = self.automation.get("CO2_off", 800)
        self.trigger_interval = self.automation.get('trigger_interval', 5)
        self.feedback_mqtt_topic = self.automation.get('feedback_mqtt_topic', "rl_correct/subiot/example/command")

        self.default_config = {
            "cratedb_config": self.cratedb_config,
            "automation": self.automation,
            "thermal_zone_mapping": self.thermal_zone_mapping,
            "CO2_on": self.CO2_on,
            "CO2_off": self.CO2_off,
            "trigger_interval": self.trigger_interval,
            "feedback_mqtt_topic": self.feedback_mqtt_topic,
        }
        self.iaq_devices = {}
        self.zones = {}

        # Set a default configuration to ensure that self.configure is called immediately to setup
        # the agent.
        self.vip.config.set_default("config", self.default_config)
        # Hook self.configure up to changes to the configuration file "config".
        self.vip.config.subscribe(self.configure, actions=["NEW", "UPDATE"], pattern="config")

    def configure(self, config_name, action, contents):
        """
        Called after the Agent has connected to the message bus. If a configuration exists at startup
        this will be called before onstart.

        Is called every time the configuration in the store changes.
        """
        config = self.default_config.copy()
        config.update(contents)

        _log.debug("Configuring Agent")

        try:
            automation = config.get("automation", dict())
            thermal_zone_mapping = config.get("thermal_zone_mapping", dict())
            cratedb_config = config.get("cratedb_config", dict())
        except ValueError as e:
            _log.error("ERROR PROCESSING CONFIGURATION: {}".format(e))
            return

        self.cratedb_config = cratedb_config
        self.automation = automation
        self.thermal_zone_mapping = thermal_zone_mapping
        
        self.CO2_on = self.automation.get("CO2_on", 1000)
        self.CO2_off = self.automation.get("CO2_off", 800)
        self.trigger_interval = self.automation.get('trigger_interval', 5)
        self.feedback_mqtt_topic = self.automation.get('feedback_mqtt_topic', "rl_correct/subiot/example/command")

        self._create_subscriptions()
        
        # trigger OAU automation function
        self.core.schedule(cron(f"*/{int(self.trigger_interval)} * * * *"), self.oau_automation)
    
    def _create_subscriptions(self):
        """
        Unsubscribe from all pub/sub topics and create a subscription to a topic in the configuration which triggers
        the _handle_publish callback
        """
        self.vip.pubsub.unsubscribe("pubsub", None, None)
        
        for zone_name, info in self.thermal_zone_mapping.items():
            zone_instance = ZoneStore(zone_name)
            zone_instance.set_oau_device_ids(info.get("oau_device_ids", list()))
            self.zones[zone_name] = zone_instance
            
            iaq_device_ids = info.get("iaq_device_ids", list())
            for device_id in iaq_device_ids:
                device_instance = DeviceStore(id=device_id)
                
                zone_instance.add_device(device_id, device_instance)
                self.iaq_devices[device_id] = device_instance
                
                _log.info(f"Subscribing to topic: sensor/tuya_air_quality/{device_id}/event")
                self.vip.pubsub.subscribe(
                    peer='pubsub',
                    prefix=f"sensor/tuya_air_quality/{device_id}/event",
                    callback=self._handle_publish
                    )
                
    def _handle_publish(self, peer, sender, bus, topic, headers, message):
        """
        Callback triggered by the subscription setup using the topic from the agent's config file
        """
        if sender == self.core.identity:
            return
        
        schema, agent_name, device, mtype = topic.split("/")
        if schema == "sensor" and mtype == "event":
            if device in self.iaq_devices:
                self.iaq_devices[device].update_data(message)

    def oau_automation(self):
        """Apply automation OAU logic considering CO2 level 
        Parameters
        - selected_zone_name: str   : selected zone name to apply OAU automation controls
                                      if `selected_zone_name` is None, apply for all zones defined in config
        """
        oau_on_zones = []
        for zone_name, zone_instance in self.zones.items():
            action, state = zone_instance.execute_automation(CO2_on=self.CO2_on, CO2_off=self.CO2_off)
            
            if action:
                _log.info(f"[ACTION] OAU status for zone `{zone_name}`: {state.value}")
                for oau_id in zone_instance.oau_device_ids:
                    self.publish(oau_id, state)

            if zone_instance.OAU_status == OAUState.ON:
                oau_on_zones.append(zone_name)

        # Log on/all oaq count
        _log.info(f"Total OAU On Zones: {len(oau_on_zones)}/{len(self.zones)}")
        _log.info(f"OAU On Zones: {oau_on_zones}")

    def publish(self, device_id, state):
        """Send control commands to MQTTAgent -> MQTTBroker -> Niagara"""
        header = {
            "requesterID": self.core.identity,
            "message_type": "command"
            }
        
        message = {
            "mode": state.value,
            "subdevice_idx": 0
        }
        topic = f"hvac/bac0hvac/{device_id}/command"
        
        self.vip.pubsub.publish(
            peer='pubsub', 
            topic=topic, 
            message=message, 
            headers=header
        )
        _log.info(f"{self.core.identity}: Published message to BACnet Agent: topic=`{topic}`, message={message}")


def main():
    """Main method called to start the agent."""
    utils.vip_main(oauagent, 
                   version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass