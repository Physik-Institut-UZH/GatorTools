#!/usr/bin/env bash

source $HOME/.bashrc
source $HOME/.localenv # Here is where the $LOCALSYS and all the other paths are defined

if [[ -z "$LOCALSYS" ]]; then
	    echo "$(date) : LOCALSYS not set!" >> "$HOME/local/var/log/cron/cron_env_error.err"
	        exit 1
fi

SESSION_NAME="GatorLiveSpectra"

CONDA_ENV="gator"
PYTHON_EXEC="$HOME/miniconda3/envs/$CONDA_ENV/bin/python"

CRON_LOG="$LOCALSYS/var/log/cron/EnsureLiveSpectra.log"
CRON_ERR="$LOCALSYS/var/log/cron/EnsureLiveSpectra.err"

GATOR_LIVE_SPECTRA_SCRIPT="gator_live_spectra.py"
SCRIPT_PATH="$LOCALSYS/bin/$GATOR_LIVE_SPECTRA_SCRIPT"
GATOR_LIVE_SPECTRA_JSON="$LOCALSYS/etc/GatorLiveSpectra/config.json"

# Check if process is running
if pgrep -f "$PYTHON_EXEC $SCRIPT_PATH" > /dev/null ; then
  echo "$(date) : Script running" >> "$CRON_LOG"
    exit 0
else
  echo "$(date) : Restarting Script" >> "$CRON_LOG"
  echo -e "\n\n$(date) : Restarting Screen session" >> "$CRON_ERR"

  screen -d -m -S "$SESSION_NAME" bash -c "
        source $HOME/.bashrc;
        source $HOME/.localenv;
        exec $PYTHON_EXEC $SCRIPT_PATH $GATOR_LIVE_SPECTRA_JSON;
  "

  exit 0
fi
