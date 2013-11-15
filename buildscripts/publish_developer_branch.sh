#!/bin/bash
#Publishes development branch to master, if it contains a tag. 
#This means you need to push with --tags option from your local repository. Otherwise the tags are not pushed.
#i.e. git tag -a v1.4;git push --tags 


{ git describe --contains HEAD; has_tag=$? || true; } # || true prevents returning non-zero status if the last commit does not have a tag assigned to it, and hence stop script execution
if [ $has_tag -eq 0 ] ; then   #return value of git describe is zero if current commit has a tag
    echo has tag
fi
