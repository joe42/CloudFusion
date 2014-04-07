#!/bin/bash
#Generates and publishes documentation from development branch to gh-pages, which is automatically shown as webpage by github, if it contains a tag.                                                      #This means you need to push with --tags option from your local repository. Otherwise the tags are not pushed.                                                                                            #i.e. git tag -a v1.4;git push --tags  

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

#install python dependencies needed to generate documentation
git checkout origin/development
{ git describe --contains HEAD; has_tag=$? || true; } # || true prevents returning non-zero status if the last commit does not have a tag assigned to it, and hence stop script execution
python setup.py install #install cloudfusion
pip install sphinx      #install sphinx

#get clone with write capability 
#GH_TOKEN is generated like this:
#apti ruby1.9.1-dev;sudo gem install travis;
#curl -X POST -u joe42 -H "Content-Type: application/json" -d "{\"scopes\":[\"public_repo\"],\"note\":\"token for pushing from travis\"}" https://api.github.com/authorizations
#travis encrypt -r joe42/CloudFusion GH_TOKEN=RETURNED_TOKEN_FROM_CURL_REQUEST --add env.global
cd $HOME
git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/joe42/CloudFusion.git  gh-pages > /dev/null
cd gh-pages

git rm -r .             #clean up

# get everything from development branch to generate documentation
git checkout origin/development -- cloudfusion

#generate documentation
cloudfusion/doc/generate_modules.py -d cloudfusion/doc -f -m 5 cloudfusion main.py dropbox cloudfusion/fuse.py cloudfusion/conf cloudfusion/doc
make -f cloudfusion/doc/Makefile html

#copy documentation to root and delete everything else
cp -r cloudfusion/doc/_build/html/_static static #copy documentation to root
cp cloudfusion/doc/_build/html/*.html .          #copy documentation to root
rm -rf cloudfusion/                              #remove files in root that have nothing to do with documentation

#push changes if last commit of development branch contains a tag
if [ $has_tag -eq 0 ] ; then   #return value of git describe is zero if current commit has a tag
    git add -A
    git commit -am "Auto update documentation from travis-ci.org"
    git push -q origin gh-pages >/dev/null
fi

#clean up
cd $HOME
#deactivate does not work in travis pre setup virtualenv, so don't stop, just if this commmand would fail
deactivate || true
rm -fr virt_env/ 
rm -fr gh-pages
cd $wd
