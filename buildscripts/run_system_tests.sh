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

sudo apt-get install -y git python-setuptools python-dev 
sudo usermod -a -G fuse "$USER"
sudo mknod /dev/fuse c 10 229
sudo chmod 666 /dev/fuse

git clone --quiet --depth 1 --branch=development https://github.com/joe42/CloudFusion.git development > /dev/null
git clone --quiet --depth 1 https://github.com/joe42/fusetests.git /tmp/fusetests > /dev/null
cd development

sudo python setup.py install

cd 

echo "Start CloudFusion."
cloudfusion --config Dropbox.ini db foreground &


sleep 10

echo "Start Test."
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

