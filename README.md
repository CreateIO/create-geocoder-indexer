# create-geocoder-indexer
Scripts to build the geocoder index


    * brew install elasticsearch

    virtual_env venv

    venv/bin/easy_install pycurl
    venv/bin/pip install -r requirements.txt

    * copy load_env.sh.sample to load_env.sh
    * edit load_env.sh
        * edit all fields with a value in < >

    * run venv/bin/python builder.py

    * apply the bulk load
        * bash batch_apply.sh


