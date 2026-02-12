import os

import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logger(logger_settings_dict:dict=None):
    
    log_dir = None
    if (not logger_settings_dict is None) and ('log_dir' in logger_settings_dict):
        log_dir = logger_settings_dict['log_dir']
        os.makedirs(log_dir, exist_ok=True)

    log_file_prefix = None

    if (not log_dir is None):
        log_file_prefix = logger_settings_dict['log_file_prefix']
    log_file = os.path.join(log_dir, f"{log_file_prefix}.log")

    logger = logging.getLogger(logger_settings_dict['logger_name'])
    logger.setLevel(logging.INFO) #This is the default

    if (not logger_settings_dict is None) and ('log_level' in logger_settings_dict):
        set_logger_level(logger, logger_settings_dict['log_level'])
    
    logger.propagate = False  # prevents duplicate prints

    # Formatter similar to dmesg (timestamp + level)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # --- File handler: rotate daily ---
    if not log_dir is None:
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
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger

def set_logger_level(logger, level):
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