#!/usr/bin/env bash

source $HOME/.localenv # Here is where the $LOCALSYS and all the other paths are defined

PROC_NAME="process_gator_trigrate.py"
SESSION_NAME="GatorTrigRate"

LOG_DIR="$LOCALSYS/var/log/gator_trigmon"
CRON_LOG="$LOCALSYS/var/log/cron/EnsureTrigMonitoring.log"

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$CRON_LOG")"


# Check if process is running
if pgrep -f "$PROC_NAME" > /dev/null ; then
  echo "$(date) : Script running" >> "$CRON_LOG"
    exit 0
else 
  echo "$(date) : Restarting Script" >> "$CRON_LOG"

  screen -d -m -S "$SESSION_NAME" bash -c "
        source ~/.bashrc
        source ~/.localenv
        
        conda activate gator

        exec python $PROC_NAME 2>> $LOG_DIR/trigmon.err 2>&1
    "

  exit 0
fi
