#!/bin/bash
#Executes tests. 
#Running sequences in background do not seem to work using paranthesis in travis-ci.
#Instead, use a subshell.

# exit if any command returns non-zero status
set -e 

wd=`pwd`

##Prepare
#setup virtualenv
cd $HOME
virtualenv virt_env
source virt_env/bin/activate || true # does not work in travis, which has its own virtualenv
cd -

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

perl -pi -e "s/user = /user = ${USR}/g" cloudfusion/config/Dropbox.ini
perl -pi -e "s/password = /password = ${PW}/g" cloudfusion/config/Dropbox.ini
perl -pi -e "s/user = /user = ${USR}/g" cloudfusion/config/sugarsync_testing.ini
perl -pi -e "s/password = /password = ${PW}/g" cloudfusion/config/sugarsync_testing.ini

#options: -x stop on first error, -v verbose, -s output stdout messgages immediately, --with-coverage produce coverage results
bash -c "nosetests -v -s -x cloudfusion/tests/db_logging_thread_test.py --with-coverage &>test1_log; mv .coverage .coverage.1; " & #about 18 Min runtime
pid1=$!
bash -c " nosetests -v -s -x cloudfusion/tests/synchronize_proxy_test.py --with-coverage &>test2_log; mv .coverage .coverage.2; " & #about 17 Min runtime
pid2=$!
nosetests -v -s -x -I db_logging_thread_test.py -I synchronize_proxy_test.py --with-coverage                             #about 20 Min runtime
mv .coverage .coverage.3

wait $pid1    #wait for test process to end
cat test1_log #and print output
(exit $?)     #set exit code to stop the script in case the test failed
wait $pid2
cat test2_log
(exit $?)

mv cloudfusion/config/Dropbox.ini.bck cloudfusion/config/Dropbox.ini
rm cloudfusion/config/sugarsync_testing.ini

coverage combine #combine coverage report
coverage html
coverage xml     #create cobertura compatible report


#clean up
cd $HOME
#deactivate does not work in travis pre setup virtualenv, so don't stop, just if this commmand would fail
deactivate || true
rm -fr virt_env/ 
rm -fr development
cd $wd