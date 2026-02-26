#!/usr/bin/env bash

source $HOME/.bashrc
source $HOME/.localenv # Here is where the $LOCALSYS and all the other paths are defined

SESSION_NAME="GatorDaqSync"

LOG_DIR="$LOCALSYS/var/log/gator_daq_sync"
CRON_LOG="$LOCALSYS/var/log/cron/EnsureDaqSync.log"
CRON_ERR="$LOCALSYS/var/log/cron/EnsureDaqSync.err"

GATOR_DAQSYNC_JSON="$LOCALSYS/etc/GatorDaqSync/config.json"

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$CRON_LOG")"


# Check if process is running
if pgrep -f "[g]ator_daq_sync.py" > /dev/null ; then
  echo "$(date) : Script running" >> "$CRON_LOG"
    exit 0
else
  echo "$(date) : Restarting Script" >> "$CRON_LOG"

  screen -d -m -S "$SESSION_NAME" bash -c "
        source ~/.bashrc
        source ~/.localenv

        conda activate gator

        exec gator_daq_sync.py $GATOR_DAQSYNC_JSON 2>> $CRON_ERR
    "

  exit 0
fi
