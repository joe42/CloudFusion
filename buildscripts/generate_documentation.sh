#!/bin/bash
# Generates and publishes documentation from development branch to gh-pages, which is automatically shown as webpage by github, if it contains a tag.                                                      
# This means you need to push with --tags option from your local repository. Otherwise the tags are not pushed.                                                                                            
# i.e. git tag -a v1.4;git push --tags  

# exit if any command returns non-zero status
set -e 

# Push changes if last commit of development branch contains a tag.
# Return value of git describe is zero if current commit has a tag.
git describe --contains HEAD

pip install sphinx      # Install sphinx.

#get clone with write capability 
#GH_TOKEN is generated like this:
#apti ruby1.9.1-dev;sudo gem install travis;
#curl -X POST -u joe42 -H "Content-Type: application/json" -d "{\"scopes\":[\"public_repo\"],\"note\":\"token for pushing from travis\"}" https://api.github.com/authorizations
#travis encrypt -r joe42/CloudFusion GH_TOKEN=RETURNED_TOKEN_FROM_CURL_REQUEST --add env.global
cd "$TRAVIS_BUILD_DIR/.."
git clone --quiet --depth 1 --branch=gh-pages https://${GH_TOKEN}@github.com/joe42/CloudFusion.git  gh-pages > /dev/null
cd gh-pages

git rm -r .             # Clean up.

# Get everything from development branch to generate documentation.
git checkout origin/development -- cloudfusion

# Generate documentation.
cloudfusion/doc/generate_modules.py -d cloudfusion/doc -f -m 5 cloudfusion main.py dropbox cloudfusion/fuse.py cloudfusion/conf cloudfusion/doc
make -f cloudfusion/doc/Makefile html

# Copy documentation to root and delete everything else.
cp -r cloudfusion/doc/_build/html/_static static # Copy documentation to root.
cp cloudfusion/doc/_build/html/*.html .          # Copy documentation to root.
rm -rf cloudfusion/                              # Remove files in root that have nothing to do with documentation.

git add -A
git commit -am "Auto update documentation from travis-ci.org"
git push -q origin gh-pages >/dev/null

cd "$TRAVIS_BUILD_DIR"
