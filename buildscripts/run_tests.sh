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

perl -pi -e "s/bucket_name =.*/bucket_name = cloudfusion/g" cloudfusion/config/Google_testing.ini
perl -pi -e "s/bucket_name =.*/bucket_name = cloudfusion/g" cloudfusion/config/AmazonS3_testing.ini

# This tests is interdependent, so wait before store_test, and store_test2 begin
nosetests -v -s -x cloudfusion/tests/transparent_store_test_no_sync.py

#options: -x stop on first error, -v verbose, -s output stdout messgages immediately
#bash -c 'nosetests -v -s -x cloudfusion/tests/db_logging_thread_test.py &>test1_log; status=$?; exit $status' & #about 18 Min runtime
#pid1=$!
#bash -c 'nosetests -v -s -x cloudfusion/tests/synchronize_proxy_test.py &>test2_log; status=$?; exit $status' & #about 17 Min runtime
#pid2=$!                          
bash -c 'nosetests -v -s -x cloudfusion/tests/store_test2.py  &>test3_log; status=$?;exit $status' &    #about 20 Min runtime                  
pid3=$!
bash -c 'nosetests -v -s -x cloudfusion/tests/store_test_webdav.py  &>test4_log; status=$?; exit $status' &                  
pid4=$!
bash -c 'nosetests -v -s -x cloudfusion/tests/store_test_webdav2.py  &>test5_log; status=$?; exit $status' &                  
pid5=$!
nosetests -v -s -x -I db_logging_thread_test.py -I synchronize_proxy_test.py -I store_test2.py  -I store_test_webdav.py  -I store_test_webdav2.py -I transparent_store_test_no_sync.py -I transparent_store_test_with_sync.py -I store_test_gdrive.py

exit_status=0
#wait $pid1    #wait for test process to end
#(exit $?)     #set exit code to stop the script in case the test failed
#cat test1_log #and print output
#wait $pid2
#(exit $?)
#cat test2_log
wait $pid3
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test3_log
wait $pid4
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test4_log
wait $pid5
status=$?
if [ "$status" -ne "0" ] ; then 
    exit_status=$status
fi
cat test5_log


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

