#!/bin/bash
#Executes tests. 
#Running sequences in background do not seem to work using paranthesis in travis-ci.
#Instead, use a subshell.

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

# The test modules must be executed sequentially, but the test cases inside the module can run concurrently
#options: -x stop on first error, -v verbose, -s output stdout messgages immediately
exit_status=0

#bash -c 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_cache_store &>test1_log; status=$?; exit $status' & 
#pid1=$!
#bash -c 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_metadata_cache_store &>test2_log; status=$?; exit $status' & 
#pid2=$!
#bash -c 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_cache_store &>test3_log; status=$?; exit $status' & 
#pid3=$!
#bash -c 'nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_metadata_cache_store &>test4_log; status=$?; exit $status' & 
#pid4=$!
#
#wait $pid1
#status=$?
#if [ "$status" -ne "0" ] ; then 
#    exit_status=$status
#fi
#cat test1_log
#wait $pid2
#status=$?
#if [ "$status" -ne "0" ] ; then 
#    exit_status=$status
#fi
#cat test2_log
#wait $pid3
#status=$?
#if [ "$status" -ne "0" ] ; then 
#    exit_status=$status
#fi
#cat test3_log
#wait $pid4
#status=$?
#if [ "$status" -ne "0" ] ; then 
#    exit_status=$status
#fi
#cat test4_log


#options: -x stop on first error, -v verbose, -s output stdout messgages immediately
#bash -c 'nosetests -v -s -x cloudfusion/tests/db_logging_thread_test.py &>test1_log; status=$?; exit $status' & #about 18 Min runtime
#pid1=$!
#bash -c 'nosetests -v -s -x cloudfusion/tests/synchronize_proxy_test.py &>test2_log; status=$?; exit $status' & #about 17 Min runtime
#pid2=$!                          
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_dropbox  &>test5_log; status=$?;exit $status' &    #about 20 Min runtime                  
pid5=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_sugarsync  &>test6_log; status=$?; exit $status' &                  
pid6=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_yandex &>test7_log; status=$?; exit $status' &                  
pid7=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_local &>test8_log; status=$?; exit $status' &                  
pid8=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_amazon &>test9_log; status=$?; exit $status' &                  
pid9=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_google &>test10_log; status=$?; exit $status' &                  
pid10=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_tonline &>test11_log; status=$?; exit $status' &                  
pid11=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_gmx &>test12_log; status=$?; exit $status' &                  
pid12=$!
bash -c 'nosetests -v -s -x cloudfusion.tests.store_tests:test_webdav_box &>test13_log; status=$?; exit $status' &                  
pid13=$!
nosetests -v -s -x -I db_logging_thread_test.py -I synchronize_proxy_test.py -I store_tests.py -I transparent_store_test_with_sync.py -I store_test_gdrive.py -I store_sync_thread.py

bash -c '(for i in {1..3} ; do sleep 180; echo -e "\nkeep alive\n"; done)' &

wait $pid5
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test5_log
wait $pid6
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test6_log
wait $pid7
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test7_log
wait $pid8
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test8_log
wait $pid9
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test9_log
wait $pid10
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test10_log
wait $pid11
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test11_log
wait $pid12
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test12_log
wait $pid13
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test13_log
wait $pid14
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test14_log


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


exit $exit_status

