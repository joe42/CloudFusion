#!/bin/bash
# Executes tests specifed by first parameter TEST_SUITE, which can either be "integration" to execute integration tests or 
# "system" to execute system tests including the fuse environment. 

TEST_SUITE="$1"
if [ "$TEST_SUITE" = "integration" ] ; then 
    bash buildscripts/run_integration_tests.sh;
    exit $?
else
    # Unpack ssh credentials to login to EC2 instance.
    tar -xf buildscripts/.ssh_and_ec2_variables.sh.tar
    echo Unpacked:
    pwd
    ls -al .ssh
    mv ec2_variables.sh buildscripts
    IP=$(bash buildscripts/start_ec2_instance.sh)
    # Copy Dropbox.ini and system test script to  EC2 instance.
    scp -r -oStrictHostKeyChecking=no -i ~/.ssh/ec2keypair.pem cloudfusion/config/Dropbox.ini ubuntu@$IP
    scp -r -oStrictHostKeyChecking=no -i ~/.ssh/ec2keypair.pem buildscripts/run_system_tests.sh; ubuntu@$IP
    ssh -oStrictHostKeyChecking=no -i ~/.ssh/ec2keypair.pem ubuntu@$IP 'bash run_system_tests.sh'
    test_result=$?
    bash buildscripts/terminate_ec2_instance.sh
    exit $test_result
fi

