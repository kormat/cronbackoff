cronbackoff
===========

Cron wrapper to skip execution after failure, providing exponential backoff.

Example
-------
    SHELL=/bin/bash
    LOG=/tmp/cronbackoff-example.log
    # m h dom mon dow   command
    */5 * * * *   $HOME/bin/cronbackoff.py -b 10 -- /path/to/example/executable -r &> "$LOG" || cat "$LOG"

Options
-------
    usage: cronbackoff.py [-h] [-b BASE_DELAY] [-m MAX_DELAY] [-e EXPONENT] [-d]
                          [-n NAME] [--state-dir STATE_DIR]
                          command [command ...]

    positional arguments:
      command               Command to run

    optional arguments:
      -h, --help            show this help message and exit
      -b BASE_DELAY, --base-delay BASE_DELAY
                            Time (in minutes) to skip execution after the first
                            failure (Default: 60 mins)
      -m MAX_DELAY, --max-delay MAX_DELAY
                            Maximum time (in minutes) to skip execution (Default:
                            1440 mins)
      -e EXPONENT, --exponent EXPONENT
                            How much to multiply the previous delay upon another
                            failure (Default: 4x)
      -d, --debug           Enable debugging output
      -n NAME, --name NAME  Name of state file. Defaults to name of command
      --state-dir STATE_DIR
                            Directory to store state in (Default: /tmp
                            /cronbackoff-USERNAME)

Installation
------------
Just copy *cronbackoff.py* to the desired location, and set executable.

Development
-----------
The test suite uses [nose](https://nose.readthedocs.org/) to run the tests. When you have nose installed, simply run *nosetests* in the top-level directory of the git checkout, and all tests will be run. E.g.:

    $ nosetests
    ...................................................
    ----------------------------------------------------------------------
    Ran 51 tests in 0.079s
    
    OK

A pylint config file is supplied, and can be used like this:

    $ pylint --rcfile=pylintrc cronbackoff.py
    $
