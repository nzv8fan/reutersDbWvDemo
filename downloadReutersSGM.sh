#!/bin/bash

# create a directory to store the sgm files in
mkdir reuters21578sgm

cd reuters21578sgm

# download the files
# based on reccommendation from: http://www.daviddlewis.com/resources/testcollections/reuters21578/
wget http://kdd.ics.uci.edu/databases/reuters21578/reuters21578.tar.gz

# extract the files
tar -xvf reuters21578.tar.gz

cd ..
