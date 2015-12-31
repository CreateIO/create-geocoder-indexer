#!/bin/bash

# package the build up and save it to AWS
set -e

dt=`date +%Y%m%d`
file="gdc_${dt}.zip"

if [ ! -r bin/batch_pre.sh ]; then
    echo "please restore data/batch_pre.sh"
    exit
fi

if [ -r data/tmp ]; then
    echo "removing working data before packaging the build"
    rm -rf data/tmp
fi
if [ -r data/batch_apply.sh -o -r data/batch_pre.sh ]; then
    echo "removing scripts from the data dir before packaging the build"
    rm -f data/batch_apply.sh data/batch_pre.sh
fi


# do not save directory paths and compress the file as much as possible
zip -j -9 dumps/${file}  data/* bin/batch_apply.sh bin/batch_pre.sh 

# save the build into AWS S3
# you may need to set the AWS_KEY and AWS_SECRET first
echo "uploading build to AWS S3"
aws s3 cp dumps/${file} s3://t-rex-dumps/geocoder/${file}

echo "now unpackage the dump into the data dir "
echo "  unzip -d data dumps/$file"
echo "then run data/batch_apply.sh to apply the data"

