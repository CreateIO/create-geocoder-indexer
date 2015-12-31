#!/bin/bash

dt=`date +%Y%m%d`

zip -9 dumps/gdc_${dt}.zip  batch_apply.sh data/* 

echo "now run batch_apply.sh to apply the data"

