cronbackoff
===========

Cron wrapper to skip execution after failure, providing exponential backoff.

Example
-------
    SHELL=/bin/bash
    LOG=/tmp/cronbackoff-example.log
    # m h dom mon dow   command
    */10 * * * *   $HOME/bin/cronbackoff /path/to/example/executable -r &> "$LOG" || cat "$LOG"
