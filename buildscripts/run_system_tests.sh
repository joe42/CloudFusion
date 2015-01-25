#!/bin/bash
# Executes system tests.

### Function definition

# Capture all output of the command passed as an argument and output it once
# the command is finished. The command is run in background.
# If the exit status is not zero, the status is written to /tmp/exit_status.
# The output is captured to a temporary file ending in .running_process_log.
capture_output () {
    TMP=$(mktemp -d).running_process_log
    bash -c "$1 &>$TMP;
        status=\$?;
        cat $TMP;
        rm $TMP; 
        if [ \$status -ne 0 ] ; then 
            echo \$status > /tmp/exit_status;
        fi;" &
}

cleanup_and_exit () {
    echo "Exit tests with return value $1."
    python -m cloudfusion.main db stop
    /tmp/fusetests/testsuite/fuse_tests.py db test
    echo $1 > "$TRAVIS_BUILD_DIR/fusetest.status"
    halt -f
}

### End function definition


insmod /usr/lib/uml/modules/`uname -r`/kernel/fs/fuse/fuse.ko

# Set up TCP/UDP network access.
ifconfig lo up
ifconfig eth0 10.0.2.15
ip route add default via 10.0.2.1

TRAVIS_BUILD_DIR="`cat /tmp/TRAVIS_BUILD_DIR`"
cd "$TRAVIS_BUILD_DIR"

python -m cloudfusion.main --config cloudfusion/config/Dropbox.ini db foreground &

# Each test runs in background, and outputs the results immediately after it has finished. 
# The script exits immediately with the exit status of nosetests if the test has failed.
capture_output '/tmp/fusetests/testsuite/fuse_tests.py db/data testfile'

# Keep travis session alive by producing output.
# If exit status of job in background is non-zero, exit.
for i in {1..42} ; do 
    sleep 60; # 1 Min
    if [ -e /tmp/exit_status ] ; then 
        exit_status=$(cat /tmp/exit_status) 
        echo 'A test failed.'
        cleanup_and_exit $exit_status
    fi
    # If there is no background job anymore:
    if ! jobs %% &>/dev/null ; then
        echo 'Tests with synchronization have finished successfully.'
        break;
    fi
    echo -e "."; 
done

# If there is still a background job:
if jobs %% &>/dev/null ; then
    echo 'Tests take too long - exiting.'
    echo "Running jobs:"
    jobs
    echo "Incomplete log files:"
    cat /tmp/*.running_process_log
    cleanup_and_exit 1
fi

cleanup_and_exit 0

