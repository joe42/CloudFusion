#!/bin/bash
# Executes tests specifed by first parameter TEST_SUITE, which can eiter be "integration" to execute integration tests or 
# "system" to execute system tests including the fuse environment. 

TEST_SUITE="$1"
if [ "$TEST_SUITE" = "integration" ] ; then 
    bash buildscripts/run_integration_tests.sh;
    exit $?
else
    git clone https://github.com/joe42/fusetests.git /tmp/fusetests
    echo "$TRAVIS_BUILD_DIR" > /tmp/TRAVIS_BUILD_DIR
    # Start User Mode Linux with root privileges 
    # to work around multiprocessing bug which requires removing /dev/shm.
    sudo /usr/bin/linux.uml init=`pwd`/buildscripts/run_system_tests.sh rootfstype=hostfs rw eth0=slirp
    exit $(<"$TRAVIS_BUILD_DIR/fusetest.status") 
fi

