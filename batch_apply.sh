#!/bin/bash

bash batch_pre.json

if [ ! -r tmp ]; then
  mkdir tmp
else
  rm tmp/bq??
fi

split -l 10000 batch.json tmp/bq

for i in tmp/bq??; do
    curl -XPOST localhost:9200/_bulk --data-binary  @${i}
    echo ""
done 


curl -XPOST 'http://localhost:9200/_aliases' -d '
{
    "actions" : [
        { "remove" : { "index" : "geodc_b", "alias" : "gdc" } },
        { "add" : { "index" : "geodc_b", "alias" : "gdc" } }
    ]
}'

curl -XPOST 'http://localhost:9200/_aliases' -d '
{
    "actions" : [
        { "add" : { "index" : "geodc", "alias" : "gdc" } }
    ]
}'

