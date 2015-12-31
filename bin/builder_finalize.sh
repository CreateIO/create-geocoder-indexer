#!/bin/bash

# package the build up and save it to AWS
set -e

dt=`date +%Y%m%d`
file="gdc_${dt}.zip"

if [ ! -r bin/batch_pre.sh ]; then
    echo "please restore data/batch_pre.sh"
    exit
fi

# do not save directory paths and compress the file as much as possible
zip -j -9 dumps/${file}  bin/batch_apply.sh bin/batch_pre.sh data/*

# save the build into AWS S3
# you may need to set the AWS_KEY and AWS_SECRET first
aws s3 cp dumps/${file} s3://t-rex-dumps/geocoder/${file}

echo "now run batch_apply.sh to apply the data"

