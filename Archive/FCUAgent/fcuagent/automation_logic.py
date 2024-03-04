import pendulum
import numpy as np
import pandas as pd

from pythermalcomfort.models import a_pmv

from .data_handler import query_data_from_database


def get_data(cratedb_config: dict(), device_ids: list, lookback: int=30):
    _now = pendulum.now(tz="Asia/Bangkok")
    _end_unix = _now.timestamp()
    _start_unix = _now.subtract(minutes=lookback).timestamp()

    # query data
    filters = {
        'timestamp': {
            '>=': _start_unix,
            '<': _end_unix
        },
        'device_id': {
            'IN': device_ids
        },
    }

    table_name = cratedb_config.get("table_name", "raw_data")
    df = query_data_from_database(cratedb_config=cratedb_config,
                                  filters=filters,
                                  table_name=table_name,
                                  pivot_datapoint_column=True)
    return df


# TODO: validate more on `a_pmv` function
def get_target_temperature(aPMV_target: float, rh: float, mrt: float=None, vr: float=0.1, met: float=1.1, clo: float=0.7, a_coefficient: float=0.2, left=False):
    setpoints = [_t for _t in range(18, 31)]
    tr = mrt
    for idx, setpoint in enumerate(setpoints):
        if mrt is None:
            tr = setpoint
        _apmv: float = a_pmv(tdb=setpoint, tr=tr, vr=vr, rh=rh, met=met, clo=clo, a_coefficient=a_coefficient, wme=0)
        if np.isnan(_apmv):
            continue
        if _apmv >= aPMV_target:
            if left:
                return setpoints[max(0, idx-1)]
            return setpoint
    return 25


def construct_control_message(fcu_device_ids: list, mode: int=1, set_temperature: float=25):
    mqtt_messages = list()
    _now = pendulum.now(tz="Asia/Bangkok")
    for fcu_device_id in fcu_device_ids:
        mqtt_messages.append({
            "topic": f"mqtt/fcu_control/{fcu_device_id}/command",
            "message": {
                "set_temperature": set_temperature,
                "mode": mode,  # options: [1 (cool), 3 (fan), 5 (dry)]
                "timestamp": _now.to_atom_string(),
                "unix_timestamp": _now.timestamp(),
                "source": "automation"
            }
        })
    return mqtt_messages


def fcu_control_logics(cratedb_config: dict(), iaq_device_ids: list, fcu_device_ids: list, aPMV_min: float=0, aPMV_target: float=0.25, aPMV_max: float=0.5,
                       rH_max: float=0.6, vr: float=0.1, met: float=1.1, clo: float=0.7, a_coefficient: float=0.2, lookback=15, fixed_humidity=50):
    # Case 1: thermal zone with no IAQ sensor
    if len(iaq_device_ids) == 0:
        try:
            # prepare FCU data
            fcu_df = get_data(cratedb_config=cratedb_config, device_ids=fcu_device_ids, lookback=30)
        except:
            iaq_df = pd.DataFrame([])
            fcu_df = pd.DataFrame([])
        
        if len(fcu_df) <= 0:
            return list(), pd.DataFrame([]), fcu_df
        
        # preprocess data
        fcu_df = fcu_df.groupby('device_id').resample('5T', label='right').agg({
            # DEDE data schema (mode, set_temperature, temperature)
            'mode': 'last',
            'set_temperature': 'last',
            'temperature': 'mean'
        }).reset_index()
        
        # check recent 30-min FCU mode
        fcu_ONs = list()
        fcu_names = fcu_df["device_id"].unique()
        for fcu_name in fcu_names:
            _fcu_modes = fcu_df[fcu_df["device_id"] == fcu_name]["mode"].values.tolist()
            # REMARK: DEDE `mode` is '"off"' (include `"` symbol) 
            if len(_fcu_modes) <= 0:
                continue
            if all("off" not in _fcu_mode for _fcu_mode in _fcu_modes):
                fcu_ONs.append(fcu_name)

        # construct control messages
        mqtt_messages = list()
        # estimate setpoint temperature from fixed humidity value (50%)
        set_temperature = get_target_temperature(aPMV_target=aPMV_target, rh=fixed_humidity, vr=vr, met=met, clo=clo, a_coefficient=a_coefficient, left=False)
        mqtt_messages: list = construct_control_message(fcu_ONs, mode=1, set_temperature=set_temperature)
        
        return mqtt_messages, pd.DataFrame([]), fcu_df

    # Case 2: thermal zone with IAQ sensor
    else:
        try:
            # prepare IAQ and FCU data
            iaq_df = get_data(cratedb_config=cratedb_config, device_ids=iaq_device_ids, lookback=lookback)
            fcu_df = get_data(cratedb_config=cratedb_config, device_ids=fcu_device_ids, lookback=lookback)
        except:
            iaq_df = pd.DataFrame([])
            fcu_df = pd.DataFrame([])
        
        if (len(iaq_df) <= 0) or (len(fcu_df) <= 0):
            return list(), iaq_df, fcu_df

        # TODO: handle missing data
        # preprocess data
        iaq_df = iaq_df.groupby('device_id').resample('5T', label='right').agg({
            'humidity': 'mean',
            'temperature': 'mean'
        }).reset_index()
        iaq_df['aPMV'] = iaq_df.apply(lambda x: a_pmv(tdb=x.temperature, tr=x.temperature, vr=vr, rh=x.humidity, met=met, clo=clo, a_coefficient=a_coefficient, wme=0), axis=1)

        fcu_df = fcu_df.groupby('device_id').resample('5T', label='right').agg({
            # DEDE data schema (mode, set_temperature, temperature)
            # Original data schema (fan, mode, temperature, set_temperature)
            'mode': 'last',
            'set_temperature': 'last',
            'temperature': 'mean'
        }).reset_index()

        # 2. identify aPMV zone
        # get current aPMV value
        aPMV_list = iaq_df["aPMV"].values.tolist()
        if len(aPMV_list) <= 0:
            # raise Exception('no data found')
            current_aPMV = np.nan
        else:
            current_aPMV = aPMV_list[-1]

        # identify current aPMV zone, options: ["PMV-A", "PMV-B", "PMV-C", "PMV-D"]
        if np.isnan(current_aPMV):  # handle case that temperature is really high ex. 30C
            current_aPMV_zone = "PMV-D"
        elif current_aPMV > aPMV_max:
            current_aPMV_zone = "PMV-D"
        elif current_aPMV > aPMV_target:
            current_aPMV_zone = "PMV-C"
        elif current_aPMV > aPMV_min:
            current_aPMV_zone = "PMV-B"
        else:
            current_aPMV_zone = "PMV-A"

        # 3. check recent X-min humidity
        # TODO: handle missing data
        humidity_mean = iaq_df["humidity"].mean()
        humidity_list = iaq_df["humidity"].values.tolist()
        if len(humidity_list) <= 0:
            # raise Exception('no data found')
            current_humidity = 55
        else:
            current_humidity = humidity_list[-1]
        if humidity_mean >= rH_max:
            if current_aPMV_zone in ["PMV-A", "PMV-B"]:
                # send dry mode control for 15 min
                mqtt_messages = construct_control_message(fcu_device_ids, mode=5)
            else:
                # send cool mode (precool) for 15 min at aPMVmin temperature
                set_temperature = get_target_temperature(aPMV_target=aPMV_min, rh=current_humidity, vr=vr, met=met, clo=clo, a_coefficient=a_coefficient, left=True)
                mqtt_messages = construct_control_message(fcu_device_ids, mode=1, set_temperature=set_temperature)
            return mqtt_messages, iaq_df, fcu_df

        # 4. check PMV comfort
        if current_aPMV_zone == "PMV-A":
            # send fan mode
            mqtt_messages = construct_control_message(fcu_device_ids, mode=3)
        elif current_aPMV_zone in ["PMV-B", "PMV-C"]:
            # send cool mode at aPMVtarget temperature
            set_temperature = get_target_temperature(aPMV_target=aPMV_target, rh=current_humidity, vr=vr, met=met, clo=clo, a_coefficient=a_coefficient, left=False)
            mqtt_messages = construct_control_message(fcu_device_ids, mode=1, set_temperature=set_temperature)
        elif current_aPMV_zone == "PMV-D":
            # send cool mode at aPMVtarget temperature
            set_temperature = get_target_temperature(aPMV_target=aPMV_target, rh=current_humidity, vr=vr, met=met, clo=clo, a_coefficient=a_coefficient, left=True)
            mqtt_messages = construct_control_message(fcu_device_ids, mode=1, set_temperature=set_temperature)

        return mqtt_messages, iaq_df, fcu_df