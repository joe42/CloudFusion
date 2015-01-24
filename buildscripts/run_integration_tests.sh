#!/bin/bash
#Executes tests. 
#Running sequences in background do not seem to work using paranthesis in travis-ci.
#Instead, use a subshell.

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

### End function definition


# Each test runs in background, and outputs the results immediately after it has finished. 
# The script exits immediately with the exit status of nosetests if the test has failed.

# nosetests options: -x stop on first error, -v verbose, -s output stdout messgages immediately

# The uncommented tests take too long on Travis CI, and are dependend on the other tests, 
# so they cannot be executed concurrently.
# capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_cache_store'
# capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_metadata_cache_store'
capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_cache_store'
capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_metadata_cache_store'

capture_output 'nosetests -v -s -x --logging-filter=dropbox cloudfusion.tests.store_tests:test_dropbox'
capture_output 'nosetests -v -s -x --logging-filter=sugarsync cloudfusion.tests.store_tests:test_sugarsync'
capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_yandex'
capture_output 'nosetests -v -s -x --logging-filter=harddrive cloudfusion.tests.store_tests:test_local'
capture_output 'nosetests -v -s -x --logging-filter=amazon cloudfusion.tests.store_tests:test_amazon'
capture_output 'nosetests -v -s -x --logging-filter=google cloudfusion.tests.store_tests:test_google'
#capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_tonline'
capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_gmx'
capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_box'
capture_output 'nosetests -v -s -x -I db_logging_thread_test.py -I synchronize_proxy_test.py -I store_tests.py -I transparent_store_test_with_sync.py -I store_test_gdrive.py'

# Keep travis session alive by producing output.
# If exit status of job in background is non-zero, exit.
for i in {1..42} ; do 
    sleep 60; # 1 Min
    if [ -e /tmp/exit_status ] ; then 
        exit_status=$(cat /tmp/exit_status) 
        echo 'A test failed.'
        exit $exit_status
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
    exit 1
fi

exit 0

