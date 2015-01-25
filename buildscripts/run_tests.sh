#!/bin/bash
# Executes tests specifed by first parameter TEST_SUITE, which can eiter be "integration" to execute integration tests or 
# "system" to execute system tests including the fuse environment. 

TEST_SUITE="$1"
if [ "$TEST_SUITE" = "integration" ] ; then 
    bash buildscripts/run_integration_tests.sh; 
else
    git clone https://github.com/joe42/fusetests.git /tmp/fusetests
    /usr/bin/linux.uml init=`pwd`/buildscripts/run_system_tests.sh rootfstype=hostfs rw
    exit $(<"$TRAVIS_BUILD_DIR/fusetest.status") 
fi

