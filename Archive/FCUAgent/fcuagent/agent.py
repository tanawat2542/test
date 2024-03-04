""" FCUAgent
Automated FCU controls based on aPMV (adaptive Predicted Mean Vote) and Tenant Feedback
The agent will consider every X minutes and will apply FCU controls to each zone defined in config file

Other agent integration:
1. `subiot` agent: to receive Tenant Feedback from LINE chatbot
2. `mqtt_logger` agent: to send control commands to Niagara via MQTTBroker

To start the agent, run the following commands:
```
vctl remove --tag fcu_control
python ~/volttron/scripts/install-agent.py -s ~/alto_os/Agents/Services/FCUAgent -t fcu_control -i fcu_control
vctl config store fcu_control config ~/alto_os/Agents/Services/FCUAgent/config
vctl enable --tag fcu_control
vctl start --tag fcu_control
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

from .automation_logic import fcu_control_logics

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"


def fcuagent(config_path, **kwargs):
    """
    Parses the Agent configuration and returns an instance of
    the agent created using that configuration.

    :param config_path: Path to a configuration file.
    :type config_path: str
    :returns: Fcuagent
    :rtype: Fcuagent
    """
    try:
        config = utils.load_config(config_path)
    except Exception:
        config = {}

    if not config:
        _log.info("Using Agent defaults for starting configuration.")

    automation = config.get("automation", dict())
    apmv = config.get("apmv", dict())
    thermal_zone_mapping = config.get("thermal_zone_mapping", dict())
    cratedb_config = config.get("cratedb_config", dict())

    return Fcuagent(automation=automation, 
                    apmv=apmv, 
                    thermal_zone_mapping=thermal_zone_mapping, 
                    cratedb_config=cratedb_config, 
                    **kwargs)


class Fcuagent(Agent):
    """
    Document agent constructor here.
    """

    def __init__(self, automation=dict(), apmv=dict(), thermal_zone_mapping=dict(), cratedb_config=dict(), **kwargs):
        super(Fcuagent, self).__init__(**kwargs)
        _log.debug("vip_identity: " + self.core.identity)

        # TODO: handle when missing parameter in Database config
        self.cratedb_config = cratedb_config

        self.automation = automation
        self.apmv = apmv
        self.thermal_zone_mapping = thermal_zone_mapping

        self.aPMV_min = self.automation.get('aPMV_min', 0)
        self.aPMV_target = self.automation.get('aPMV_target', 0.25)
        self.aPMV_max = self.automation.get('aPMV_max', 0.5)
        self.rH_max = self.automation.get('rH_max', 60)
        self.trigger_interval = self.automation.get('trigger_interval', 15)
        self.lookback_interval = self.automation.get('lookback_interval', 15)
        self.feedback_expired_minutes = self.automation.get('feedback_expired_minutes', 30)
        self.feedback_mqtt_topic = self.automation.get('feedback_mqtt_topic', "rl_correct/subiot/example/command")
        self.vr = self.apmv.get('vr', 0.1)
        self.met = self.apmv.get('met', 1.1)
        self.clo = self.apmv.get('clo', 0.65)
        self.a_coefficient = self.apmv.get('a_coefficient', 0.2)
        
        # FCU setpoint offset based on recent tenant feedbacks
        self.setpoint_offset = dict()
        for zone_name in self.thermal_zone_mapping.keys():
            self.setpoint_offset[zone_name] = 0
        
        # FCU random offset to handle the case that Niagara didn't execute redundant commands
        self.setpoint_random_offset_options = [0.1, 0.2]  # select small value so that Niagara will always ceil-round the setpoint value
        self.setpoint_random_offset_state = False

        self.default_config = {
            "cratedb_config": self.cratedb_config,
            "automation": self.automation,
            "apmv": self.apmv,
            "thermal_zone_mapping": self.thermal_zone_mapping,
            "aPMV_min": self.aPMV_min,
            "aPMV_target": self.aPMV_target,
            "aPMV_max": self.aPMV_max,
            "rH_max": self.rH_max,
            "trigger_interval": self.trigger_interval,
            "lookback_interval": self.lookback_interval,
            "feedback_expired_minutes": self.feedback_expired_minutes,
            "feedback_mqtt_topic": self.feedback_mqtt_topic,
            "vr": self.vr,
            "met": self.met,
            "clo": self.clo,
            "a_coefficient": self.a_coefficient,
            "setpoint_offset": self.setpoint_offset
        }

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
            apmv = config.get("apmv", dict())
            thermal_zone_mapping = config.get("thermal_zone_mapping", dict())
            cratedb_config = config.get("cratedb_config", dict())
        except ValueError as e:
            _log.error("ERROR PROCESSING CONFIGURATION: {}".format(e))
            return

        self.cratedb_config = cratedb_config
        self.automation = automation
        self.apmv = apmv
        self.thermal_zone_mapping = thermal_zone_mapping
        
        self.aPMV_min = self.automation.get('aPMV_min', 0)
        self.aPMV_target = self.automation.get('aPMV_target', 0.25)
        self.aPMV_max = self.automation.get('aPMV_max', 0.5)
        self.rH_max = self.automation.get('rH_max', 60)
        self.trigger_interval = self.automation.get('trigger_interval', 15)
        self.lookback_interval = self.automation.get('lookback_interval', 15)
        self.feedback_expired_minutes = self.automation.get('feedback_expired_minutes', 30)
        self.feedback_mqtt_topic = self.automation.get('feedback_mqtt_topic', "rl_correct/subiot/example/command")
        self.vr = self.apmv.get('vr', 0.1)
        self.met = self.apmv.get('met', 1.1)
        self.clo = self.apmv.get('clo', 0.65)
        self.a_coefficient = self.apmv.get('a_coefficient', 0.2)
        
        # reset setpoint offset
        self.setpoint_offset = dict()
        for zone_name in self.thermal_zone_mapping.keys():
            self.setpoint_offset[zone_name] = 0
        
        # update state for tenant feedback in each thermal zone
        """ schema
        self.tenant_feedback_states = {
            "zone_1": {
                "Too Hot": [{"unix_timestamp": 1234567890, "line_id": "U123123123"},
                            {"unix_timestamp": 1234567890, "line_id": "U231231231"}],
                "Too Cold": []
            }
        }
        """
        self.tenant_feedback_states = dict()
        for zone_name in self.thermal_zone_mapping.keys():
            self.tenant_feedback_states[str(zone_name)] = {"Too Hot": list(), "Too Cold": list()}
        _log.debug(f"{self.core.identity}: initialized self.tenant_feedback_states={self.tenant_feedback_states}")

        self._create_subscriptions()
        
        # trigger FCU automation function
        self.core.schedule(cron(f"*/{int(self.trigger_interval)} * * * *"), self.fcu_automation)
        self.core.schedule(cron("0 */2 * * *"), self._periodic_check_feedback_states)  # recheck and update feedback states every X hours

    def _create_subscriptions(self):
        """
        Unsubscribe from all pub/sub topics and create a subscription to a topic in the configuration which triggers
        the _handle_publish callback
        """
        self.vip.pubsub.unsubscribe("pubsub", None, None)

        # handle message from `subiot` agent on Tenant feedback
        self.vip.pubsub.subscribe(peer='pubsub',
                                prefix=self.feedback_mqtt_topic,
                                callback=self._handle_tenant_feedback)
        
        # TODO: handle message from `room` agent on FCU IoT data from tenant manual controls
        # REMARK: DEDE zone names not fully sync with LineOA Tenant Feedback zone names yet
        self.vip.pubsub.subscribe(peer='pubsub',
                                prefix=self.feedback_mqtt_topic,
                                callback=self._handle_tenant_feedback)

    def _handle_tenant_feedback(self, peer, sender, bus, topic, headers, message):
        """
        Callback triggered by the subscription setup using the topic from the agent's config file
        
        message (JSON) = {
            "feedback": "Too Hot", 
            "building": "BGrimm", 
            "zone": "Floor 1 Creative Arena Room", 
            "lineId": "U87d0284d6b783f8fbb4af8bd050ba1e6", 
            "feedbackId": "406",
            "topic": "human_feedback"
        }
        """
        
        if isinstance(message, str):
            message = json.loads(message)

        # validate message payload
        if ("feedback" not in message.keys()) or ("zone" not in message.keys()) or ("lineId" not in message.keys()):
            _log.exception(f"{self.core.identity}: Invalid message payload from Tenant Feedback: {message}")
            return

        feedback = message.get("feedback")
        zone_name = message.get("zone")
        line_id = message.get("lineId")
        
        _log.debug(f"{self.core.identity}: Received Tenant Feedback: {message}")
        
        # update to `self.tenant_feedback_states`
        self._remove_expired_feedbacks(zone_name, self.feedback_expired_minutes)  # handle expired feedbacks
        self._append_new_feedback(zone_name, feedback, line_id)  # append new feedback

        # calculate FCU offset of the selected zone
        self._update_fcu_setpoint_offset(zone_name)
        
        # trigger FCU automation control: apply FCU setpoint offset, construct MQTT messages, and send to MQTTAgent
        # TODO: update FCU setpoint based on current setpoint value (currently recalculate again from aPMV in `fcu_control_logics` function)
        self.fcu_automation(selected_zone_name=zone_name)
    
    def _remove_expired_feedbacks(self, zone_name, expired_minutes: int=30):
        """Remove expired feedbacks from `self.tenant_feedback_states` when the feedback is older than 30 minutes"""
        _now = pendulum.now(tz="Asia/Bangkok")
        _tenant_feedback_state = self.tenant_feedback_states.get(str(zone_name), dict())

        for feedback_type, feedbacks in _tenant_feedback_state.items():
            for feedback in feedbacks:
                _feedback_time = pendulum.from_timestamp(feedback.get("unix_timestamp"), tz="Asia/Bangkok")
                _diff = _now.diff(_feedback_time)
                if _diff.in_minutes() > expired_minutes:
                    _tenant_feedback_state[feedback_type].remove(feedback)

        # update feedback state
        self.tenant_feedback_states[str(zone_name)] = _tenant_feedback_state

    def _append_new_feedback(self, zone_name, feedback_type, line_id):
        """Append new feedback to `self.tenant_feedback_states` when the feedback is not in the list"""
        _now = pendulum.now(tz="Asia/Bangkok")
        _tenant_feedback_state = self.tenant_feedback_states.get(str(zone_name), dict())

        # check if feedback_type is valid
        if _tenant_feedback_state.get(feedback_type) is None:
            _tenant_feedback_state = {"Too Hot": list(), "Too Cold": list()}
            _log.exception(f"{self.core.identity}: invalid feedback zone name: zone_name={zone_name} feedback_type={feedback_type}")

        # check if the same line_id has already given feedback
        if line_id not in [feedback.get("line_id") for feedback in _tenant_feedback_state[feedback_type]]:
            _tenant_feedback_state[feedback_type].append({"unix_timestamp": _now.timestamp(), "line_id": line_id})

        # update feedback state
        self.tenant_feedback_states[str(zone_name)] = _tenant_feedback_state

    def _update_fcu_setpoint_offset(self, zone_name):
        zone_tenant_feedback = self.tenant_feedback_states.get(zone_name)
        if zone_tenant_feedback is None:
            _log.exception(f"{self.core.identity}: (_update_fcu_setpoint_offset) invalid feedback zone name: zone_name={zone_name}")
            return
        # calculate FCU setpoint offset
        count_hot = zone_tenant_feedback.get("Too Hot", list())
        count_cold = zone_tenant_feedback.get("Too Cold", list())
        if len(count_hot) > len(count_cold):
            self.setpoint_offset[zone_name] = -1
        elif len(count_hot) < len(count_cold):
            self.setpoint_offset[zone_name] = 1
        else:
            self.setpoint_offset[zone_name] = 0

    # TODO: update `a_coefficient` from Tenant Feedback
    def fcu_automation(self, selected_zone_name: str=None):
        """Apply automation FCU logic considering on PMV and Tenant Feedback
        Parameters
        - selected_zone_name: str   : selected zone name to apply FCU automation controls
                                      if `selected_zone_name` is None, apply for all zones defined in config
        """

        for zone_name, device_infos in self.thermal_zone_mapping.items():
            # consider only selected zones: all zones or 1 zone
            if (selected_zone_name is not None) and (zone_name != selected_zone_name):
                continue

            # update tenant feedback states (`fcu_automation` trigger from `_handle_tenant_feedback` alreday handle feedback-state update)
            if selected_zone_name is None:
                self._remove_expired_feedbacks(zone_name)  # handle expired feedbacks
                self._update_fcu_setpoint_offset(zone_name)  # calculate FCU offset

            # IoT devices in 1 Thermal Zone
            iaq_device_ids = device_infos.get("iaq_device_ids", list())
            fcu_device_ids = device_infos.get("fcu_device_ids", list())

            # TODO: handle case that can't access CrateDB cloud database
            # REMARK: FCU's datapoint names in DEDE and Synergy is different, please check carefully before deployment
            # REMARK: DEDE zone names not fully sync with LineOA Tenant Feedback zone names yet
            mqtt_messages, _, _ = fcu_control_logics(cratedb_config=self.cratedb_config,
                                                     iaq_device_ids=iaq_device_ids,
                                                     fcu_device_ids=fcu_device_ids,
                                                     aPMV_min=self.aPMV_min,
                                                     aPMV_target=self.aPMV_target,
                                                     aPMV_max=self.aPMV_max,
                                                     rH_max=self.rH_max,
                                                     vr=self.vr,
                                                     met=self.met,
                                                     clo=self.clo,
                                                     a_coefficient=self.a_coefficient,
                                                     lookback=self.lookback_interval,
                                                     fixed_humidity=50)

            # update FCU setpoint from offset value
            fcu_setpoit_offset = self.setpoint_offset.get(zone_name, 0)
            
            # select setpoint random offset value, options: [0.1, 0.2]
            setpoint_random_offset = self.setpoint_random_offset_options[int(self.setpoint_random_offset_state)]

            # apply FCU setpoint offset
            # TODO: error handling on invalid MQTT message
            for mqtt_message in mqtt_messages:
                # set lower-bound action setpoint constraint
                _message = mqtt_message.get("message", dict())
                if _message["set_temperature"] <= 24:
                    _message["set_temperature"] = 24
                _message["set_temperature"] += fcu_setpoit_offset
                
                # apply random offset to prevent redundant commands on Niagara
                _message["set_temperature"] += setpoint_random_offset
                mqtt_message["message"] = _message

            # publish command message to MQTTAgent -> MQTTBroker -> Niagara
            self.send_control_commands(mqtt_messages)
            
            # switch `setpoint_random_offset` state (betwen 0.1 <-> 0.2)
            self.setpoint_random_offset_state = not self.setpoint_random_offset_state

    def send_control_commands(self, mqtt_messages: list):
        """Send control commands to MQTTAgent -> MQTTBroker -> Niagara"""
        _header = {"requesterID": self.core.identity,
                  "message_type": "command"}

        for mqtt_message in mqtt_messages:
            _topic_name = mqtt_message.get("topic", None)
            _message = mqtt_message.get("message", None)
            
            if _topic_name is None or _message is None:
                _log.error(f"Invalid MQTT control message from FCUAgent: topic=`{_topic_name}`, message={_message}")
                continue
            
            self.vip.pubsub.publish(
                peer='pubsub', 
                topic=str(_topic_name), 
                message=_message, 
                headers=_header
            )
            _log.info(f"{self.core.identity}: Published message to MQTTAgent: topic=`{_topic_name}`, message={_message}")

    def _periodic_check_feedback_states(self):
        """Periodically check feedback states and remove expired feedbacks, prevent memory leak.
        Will be trigger once every X hour.
        """
        for zone_name in self.tenant_feedback_states.keys():
            self._remove_expired_feedbacks(zone_name, self.feedback_expired_minutes)


def main():
    """Main method called to start the agent."""
    utils.vip_main(fcuagent, 
                   version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass