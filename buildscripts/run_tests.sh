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

cleanup_and_exit () {
    echo "Exit tests with return value $1."
    mv cloudfusion/config/Dropbox.ini.bck cloudfusion/config/Dropbox.ini
    rm cloudfusion/config/sugarsync_testing.ini
    rm cloudfusion/config/Google_testing.ini
    rm cloudfusion/config/AmazonS3_testing.ini
    rm cloudfusion/config/Webdav_gmx_testing.ini
    rm cloudfusion/config/Webdav_tonline_testing.ini

    rm cloudfusion/config/Webdav_box_testing.ini
    rm cloudfusion/config/Webdav_yandex_testing.ini

    #clean up
    cd $HOME
    rm -fr development
    
    exit $1
}

### End function definition

#configure git user for push
git config --global user.email "travis@travis-ci.org"
git config --global user.name "Travis" 

#install fuse
sudo apt-get install fuse-utils
#set shared memory permissions for muliprocessing module 
sudo rm -rf /dev/shm && sudo ln -s /run/shm /dev/shm

#install python dependencies
git checkout origin/development
python setup.py install #install cloudfusion
pip install coverage    #install coverage to get test coverage

#get clone with write capability 
#GH_TOKEN is generated like this:
#apti ruby1.9.1-dev;sudo gem install travis;
#curl -X POST -u joe42 -H "Content-Type: application/json" -d "{\"scopes\":[\"public_repo\"],\"note\":\"token for pushing from travis\"}" https://api.github.com/authorizations
#travis encrypt -r joe42/CloudFusion GH_TOKEN=RETURNED_TOKEN_FROM_CURL_REQUEST --add env.global
cd $HOME
git clone --quiet --branch=development https://${GH_TOKEN}@github.com/joe42/CloudFusion.git  development > /dev/null
cd development

cp cloudfusion/config/Sugarsync.ini cloudfusion/config/sugarsync_testing.ini
cp cloudfusion/config/Dropbox.ini cloudfusion/config/Dropbox.ini.bck #backup config file
cp cloudfusion/config/Google.ini cloudfusion/config/Google_testing.ini
cp cloudfusion/config/AmazonS3.ini cloudfusion/config/AmazonS3_testing.ini

perl -pi -e "s/user = /user = ${USR}/g" cloudfusion/config/Dropbox.ini
perl -pi -e "s/password = /password = ${PW}/g" cloudfusion/config/Dropbox.ini
perl -pi -e "s/user = /user = ${USR}/g" cloudfusion/config/sugarsync_testing.ini
perl -pi -e "s/password = /password = ${PW}/g" cloudfusion/config/sugarsync_testing.ini
perl -pi -e "s/client_id =.*/client_id =${GS_ID}/g" cloudfusion/config/Google_testing.ini
perl -pi -e "s/client_secret =.*/client_secret =${GS_KEY}/g" cloudfusion/config/Google_testing.ini
perl -pi -e "s/access_key_id =.*/access_key_id =${S3_ID}/g" cloudfusion/config/AmazonS3_testing.ini
perl -pi -e "s/secret_access_key =.*/secret_access_key =${S3_KEY}/g" cloudfusion/config/AmazonS3_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV_USR}/g" cloudfusion/config/Webdav_tonline_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV_PWD}/g" cloudfusion/config/Webdav_tonline_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV2_USR}/g" cloudfusion/config/Webdav_gmx_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV2_PWD}/g" cloudfusion/config/Webdav_gmx_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV3_USR}/g" cloudfusion/config/Webdav_box_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV3_PWD}/g" cloudfusion/config/Webdav_box_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV4_USR}/g" cloudfusion/config/Webdav_yandex_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV4_PWD}/g" cloudfusion/config/Webdav_yandex_testing.ini

perl -pi -e "s/bucket_name =.*/bucket_name = cloudfusion42/g" cloudfusion/config/Google_testing.ini
perl -pi -e "s/bucket_name =.*/bucket_name = cloudfusion42/g" cloudfusion/config/AmazonS3_testing.ini

# The test modules must be executed sequentially, but the test cases inside the module can run concurrently.
# Each test runs in background, and outputs the results immediately after it has finished. 
# The script exits immediately with the exit status of nosetests if the test has failed.

# nosetests options: -x stop on first error, -v verbose, -s output stdout messgages immediately

# capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_cache_store'
# capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_metadata_cache_store'
capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_cache_store'
capture_output 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_metadata_cache_store'

# Keep travis session alive by producing output for 20 minutes.
# If exit status of job in background is non-zero, exit.
for i in {1..30} ; do 
    sleep 60; # 1 Min
    if [ -e /tmp/exit_status ] ; then 
        exit_status=$(cat /tmp/exit_status) 
        echo 'A test failed.'
        cleanup_and_exit $exit_status
    fi
    # If there is no background job anymore:
    if ! jobs %% &>/dev/null ; then
        echo 'TransparentStore tests with synchronization have finished successfully.'
        break;
    fi
    echo -e "."; 
done

# If there is still a background job:
if jobs %% &>/dev/null ; then
    echo 'TransparentStore tests take too long - exiting.'
    echo "Running jobs:"
    jobs
    echo "Incomplete log files:"
    cat /tmp/*.running_process_log
    cleanup_and_exit 1
fi

capture_output 'nosetests -v -s -x --logging-filter=dropbox cloudfusion.tests.store_tests:test_dropbox'
capture_output 'nosetests -v -s -x --logging-filter=sugarsync cloudfusion.tests.store_tests:test_sugarsync'
capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_yandex'
capture_output 'nosetests -v -s -x --logging-filter=harddrive cloudfusion.tests.store_tests:test_local'
capture_output 'nosetests -v -s -x --logging-filter=amazon cloudfusion.tests.store_tests:test_amazon'
capture_output 'nosetests -v -s -x --logging-filter=google cloudfusion.tests.store_tests:test_google'
# capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_tonline' # This test takes too long on Travis CI.
capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_gmx'
capture_output 'nosetests -v -s -x --logging-filter=webdav cloudfusion.tests.store_tests:test_webdav_box'
capture_output 'nosetests -v -s -x -I db_logging_thread_test.py -I synchronize_proxy_test.py -I store_tests.py -I transparent_store_test_with_sync.py -I store_test_gdrive.py'


# Keep travis session alive by producing output for 20 minutes.
# If exit status of job in background is non-zero, exit.
for i in {1..20} ; do 
    sleep 60; # 1 Min
    if [ -e /tmp/exit_status ] ; then 
        exit_status=$(cat /tmp/exit_status) 
        echo 'A test failed.'
        cleanup_and_exit $exit_status
    fi
    # If there is no background job anymore:
    if ! jobs %% &>/dev/null ; then
        sleep 10;
        echo 'All tests have finished successfully.'
        cleanup_and_exit 0
    fi
    echo -e "."; 
done

echo "Waited over 20 minutes for tests to finish - exiting."
echo "Running jobs:"
jobs
echo "Incomplete log files:"
cat /tmp/*.running_process_log

cleanup_and_exit 1

