#!/usr/bin/env bash

source $HOME/.bashrc
source $HOME/.localenv # Here is where the $LOCALSYS and all the other paths are defined

SESSION_NAME="GatorTrigRate"

LOG_DIR="$LOCALSYS/var/log/gator_trigmon"
CRON_LOG="$LOCALSYS/var/log/cron/EnsureTrigMonitoring.log"
CRON_ERR="$LOCALSYS/var/log/cron/EnsureTrigMonitoring.err"

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$CRON_LOG")"


# Check if process is running
if pgrep -f "[p]rocess_gator_trigrate.py" > /dev/null ; then
  echo "$(date) : Script running" >> "$CRON_LOG"
    exit 0
else 
  echo "$(date) : Restarting Script" >> "$CRON_LOG"

  screen -d -m -S "$SESSION_NAME" bash -c '
        source ~/.bashrc
        source ~/.localenv
        
        conda activate gator

        exec process_gator_trigrate.py 2>> '"$CRON_ERR"'
    '

  exit 0
fi
