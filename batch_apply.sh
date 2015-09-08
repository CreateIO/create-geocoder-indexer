#!/bin/bash

if [ -z "$ES_Index" ]; then
    echo "usage:   batch_apply.sh"
    echo "    set the ENV var ES_Index before running this script"
    echo "    see load_env.sh for one way to do this"
    exit
fi

echo "Starting to apply ElasticSearch Index $ES_Index"
echo ""

if [ -d "data" ]; then
    cd data
fi

#scan for idxname
idxname=`head -n 1 batch_pre.sh | cut -d'=' -f2`

if [ "$ES_Index" != "$idxname" ]; then
    echo "Fatal Error"
    echo "batch index files were built for $idxname, but we are attempting to apply them to $ES_Index"
    exit
fi

bash batch_pre.sh

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
        { "remove" : { "index" : "'$ES_Index'", "alias" : "gdc" } },
        { "add" : { "index" : "'$ES_Index'", "alias" : "gdc" } }
    ]
}'

curl -XPOST 'http://localhost:9200/_aliases'

exit

curl -XPOST 'http://localhost:9200/_aliases' -d '
{
    "actions" : [
        { "add" : { "index" : "geodc", "alias" : "gdc" } }
    ]
}'

