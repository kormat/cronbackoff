cronbackoff
===========

Cron wrapper to cancel execution after failure, and provide exponential backoff

Example
-------
    SHELL=/bin/bash
    LOG=/tmp/cronbackoff-example.log
    # m h dom mon dow   command
    */10 * * * *   $HOME/bin/cronbackoff /path/to/example/executable -r &> "$LOG" || cat "$LOG"

