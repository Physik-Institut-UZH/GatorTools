import os
import sys
import numpy as np
import pandas as pd

import logging
from logging import Logger
from logging.handlers import TimedRotatingFileHandler


def load_processed_file(proc_fpath, logger:Logger=None, strict:bool=True):

    ret_dict = {}

    try:
        import_data = np.load(proc_fpath, allow_pickle=True).item()
    except Exception as err:
        if logger:
            logger.exception(f'Failed to load file "{proc_fpath}"')
        else:
            print(f'Failed to load file "{proc_fpath}": {err}', file=sys.stderr)
        raise
    #

    try:
        df_payload = import_data["Data"]
        cols  = df_payload["Cols"]
        types = df_payload["Types"]
        arr   = df_payload["Arr"]
    #
        
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the dataframe form file "{proc_fpath}"')
        else:
            print(f'Failed to load the dataframe from file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        df = pd.DataFrame(arr, columns=cols)
        for col, dtype_str in types.items():
            df[col] = df[col].astype(dtype_str)
        #
        ret_dict['df'] = df
    #
    
    try:
        daqsettings = import_data['DaqSettings']
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the DAQ settings from file "{proc_fpath}"')
        else:
            print(f'Failed to load the DAQ settings form file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        ret_dict['DaqSettings'] = daqsettings
    #

    try:
        procsettings = import_data['ProcSettings']
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the waveforms processor settings from file "{proc_fpath}"')
        else:
            print(f'Failed to load the waveforms processor settings form file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        ret_dict['ProcSettings'] = procsettings
    #

    try:
        wfslen = int(import_data['WfsLength'])
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the waveforms lenght parameter from file "{proc_fpath}"')
        else:
            print(f'Failed to load the waveforms lenght parameter from file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        ret_dict['WfsLength'] = wfslen
    #

    try:
        start_unixtime = import_data['StartUnixTime']
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the start unixtime from file "{proc_fpath}"')
        else:
            print(f'Failed to load the start unixtime from file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        ret_dict['StartUnixTime'] = start_unixtime
    #

    try:
        stop_unixtime = import_data['StopUnixTime']
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the stop unixtime from file "{proc_fpath}"')
        else:
            print(f'Failed to load the stop unixtime from file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        ret_dict['StopUnixTime'] = stop_unixtime
    #

    try:
        runtime = import_data['FileRunTime']
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the file runtime from file "{proc_fpath}"')
        else:
            print(f'Failed to load the file runtime from file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        ret_dict['FileRunTime'] = runtime
    #

    try:
        samp_freq = import_data['SampFreq']
    except Exception as err:
        if logger is not None:
            logger.exception(f'Failed to load the sampling frequency from file "{proc_fpath}"')
        else:
            print(f'Failed to load the sampling frequency from file "{proc_fpath}": {err}', file=sys.stderr)
        #
        if strict:
            raise
        #
    else:
        ret_dict['SampFreq'] = samp_freq
    #

    return ret_dict
#

def setup_logger(logger_settings_dict:dict|None=None):
    
    log_dir = None
    if (not logger_settings_dict is None) and ('log_dir' in logger_settings_dict):
        log_dir = logger_settings_dict['log_dir']
        os.makedirs(log_dir, exist_ok=True)

    log_file_prefix = None

    log_file = None
    if (not log_dir is None):
        log_file_prefix = logger_settings_dict['log_file_prefix']
        log_file = os.path.join(log_dir, f"{log_file_prefix}.log")
    #

    if (not logger_settings_dict is None):
        logger = logging.getLogger(logger_settings_dict['logger_name'])
        logger.setLevel(logging.INFO) #This is the default
    else:
        logger = logging.getLogger('default')
    #

    if (not logger_settings_dict is None) and ('log_level' in logger_settings_dict):
        set_logger_level(logger, logger_settings_dict['log_level'])
    else:
        set_logger_level(logger, 'DEBUG')
    #
    
    logger.propagate = False  # prevents duplicate prints

    # Formatter similar to dmesg (timestamp + level)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # --- File handler: rotate daily ---
    if not log_file is None:
        fh = TimedRotatingFileHandler(
            log_file,
            when="midnight",     # rotate every day at 00:00
            interval=1,
            backupCount=365,      # keep last 365 days (optional)
            utc=False            # use local time
            )
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    #

    # --- Screen handler ---
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger
#

def set_logger_level(logger:Logger, level):
    """
    Set the logging level, accepting either string (case-insensitive) or int.
    """
    if isinstance(level, str):
        level = level.upper()
        if level not in logging._nameToLevel:
            raise ValueError(f"Invalid log level string: {level}")
    elif isinstance(level, int):
        if level not in {0, 10, 20, 30, 40, 50}:
            raise ValueError(f"Invalid log level integer: {level}")
    else:
        raise TypeError(f"log_level must be str or int, got {type(level)}")

    logger.setLevel(level)
#