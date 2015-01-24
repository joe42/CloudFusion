#!/bin/bash
# Publishes development branch to master, if it contains a tag. 
# This means you need to push with --tags option from your local repository. Otherwise the tags are not pushed.
# I.e. git tag -a v1.4;git push --tags 

# Exit if any command returns non-zero status.
set -e 

## Prepare
# Configure git user for push.
git config --global user.email "travis@travis-ci.org"
git config --global user.name "Travis" 

cd "$TRAVIS_BUILD_DIR/.."
git clone --quiet --depth 1 --branch=development https://${GH_TOKEN}@github.com/joe42/CloudFusion.git development > /dev/null
cd development

git describe --contains HEAD

git push -q origin development:master >/dev/null

cd "$TRAVIS_BUILD_DIR"
