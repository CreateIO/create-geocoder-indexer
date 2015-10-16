# create-geocoder-indexer
Scripts to build the geocoder index

### install base software
1. brew install elasticsearch
1. brew install virtual_env

### setup python virtual environment
1. virtual_env venv
1. venv/bin/easy_install pycurl
1. venv/bin/pip install -r requirements.txt

### setup environment variables
1. copy load_env.sh.sample to load_env.sh
1. edit load_env.sh
1. edit all fields with a value in < >

### run the indexer
* run venv/bin/python builder.py

### apply the builk load
* bash batch_apply.sh


