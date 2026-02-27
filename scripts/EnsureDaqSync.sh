#!/usr/bin/env bash

source $HOME/.bashrc
source $HOME/.localenv # Here is where the $LOCALSYS and all the other paths are defined

SESSION_NAME="GatorDaqSync"

CONDA_ENV="gator"
PYTHON_EXEC="$HOME/miniconda3/envs/$CONDA_ENV/bin/python"

CRON_LOG="$LOCALSYS/var/log/cron/EnsureDaqSync.log"
CRON_ERR="$LOCALSYS/var/log/cron/EnsureDaqSync.err"

GATOR_DAQ_SYNC_SCRIPT="gator_daq_sync.py"
SCRIPT_PATH="$LOCALSYS/bin/$GATOR_DAQ_SYNC_SCRIPT"
GATOR_DAQSYNC_JSON="$LOCALSYS/etc/GatorDaqSync/config.json"

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
	PTY=\$(tty)
	exec $PYTHON_EXEC $SCRIPT_PATH $GATOR_DAQSYNC_JSON;
    "

  exit 0
fi
