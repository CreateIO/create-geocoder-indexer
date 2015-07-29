#!/usr/bin/python

import contextlib
import simplejson as json
import logging
import os
import sys
import psycopg2
import pycurl
import cStringIO
import time
import re

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', level=logging.DEBUG)

DB_USER = os.environ.get('CREATE_DB_USER')
DB_PASS = os.environ.get('CREATE_DB_PASS')

DB_NAME = os.environ.get('DB_NAME')
DB_INSTANCE = os.environ.get('DB_INSTANCE')
DBHOST = os.environ.get('DBHOST')
DB_PORT = os.environ.get('DB_PORT')

if not DB_USER:
    sys.stderr.write('User not found: please set environment variable CREATE_DB_USER\n')
    sys.exit(-1)

if not DB_PASS:
    sys.stderr.write('Password not found: please set environment variable CREATE_DB_PASS\n')
    sys.exit(-1)

if not DB_INSTANCE:
    DB_INSTANCE = 'test'

if not DBHOST:
    DBHOST = 'localhost'

if not DB_PORT:
    DB_PORT = '5432'

if DB_NAME:
    logger.info("using DB_NAME from environment vars")
elif 'GIT_BRANCH' in os.environ:
    DB_NAME = os.environ.get('GIT_BRANCH').split('/')[1]
else:
    DB_NAME = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip()

#Set DB connection info
logger.info('writing to database "%s" in instance "%s"', DB_NAME, DB_INSTANCE)
if DB_INSTANCE != 'test':
    DB_CONNECTION_STRING = 'host=%s.cvwdsktow3o7.us-east-1.rds.amazonaws.com dbname=%s user=%s password=%s' % (DB_INSTANCE, DB_NAME, DB_USER, DB_PASS)
else:
    DB_CONNECTION_STRING = 'host=%s dbname=%s user=%s password=%s port=%s' % (DBHOST, DB_NAME, DB_USER, DB_PASS, DB_PORT)
# print the connection string we will use to connect
print "Connecting to database\n ->%s" % (DB_CONNECTION_STRING)

@contextlib.contextmanager
def db_cursor(encoding=None):
    ''' Connect to psycoPg2 synchronously '''
    with psycopg2.connect(DB_CONNECTION_STRING, async=0) as conn:
        # Don't even think about running this stuff without turning on autocommit. You'll be waiting a loooooong time.
        conn.set_session(autocommit=True)
        if encoding:
            conn.set_client_encoding(encoding)
        with conn.cursor() as cursor:
            ''' don't wait on the WAL commit before moving on '''
            '''  this is a setting for performance, not for data security '''
            cursor.execute('set synchronous_commit=off')
            yield cursor

def drop_index(index):
    
    if index == None or index == '':
        return "Failed"
    
    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:9200/' + str(index)
    
    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    c.setopt(pycurl.CUSTOMREQUEST, "DELETE")
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
    except:
        e = sys.exc_info()[0]
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()
    pass

def send_mapping(address):

    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:9200/geodc/address'
    data = json.dumps(address)
    logger.debug(address)
    
    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, data)
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
    except:
        e = sys.exc_info()[0]
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()
    pass
    
def send_address(address):

    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:9200/geodc/address/' + str(address["id"])
    data = json.dumps(address)
    logger.debug(address)
    
    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, data)
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
    except:
        e = c.errstr()
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()
    pass

def set_address_mapping():
    map_str = '''{ "mappings": {
        "address": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "address_number": { "type": "integer" },
                "city": { "type": "string" },
                "state": { "type": "string" },
                "zipcode": { "type": "string" },
                "neighborhood": { "type": "string" },
                "addr_use": { "type": "string" }
                }
            }
        }
    }
    '''
    
    mapping = json.loads(map_str)
    
    send_mapping(mapping)
    return mapping
    
def core_address(address):
    # strip leading  Directionality
    #address = re.sub(r'^NORTH ',  '',  address)
    
    # strip tailing Quadrant data
    address = re.sub(r' (NE|NW|SE|SW)$',  '',  address)
    
    return address
    
def alt_core_address(address):
    # strip leading  Directionality
    #address = re.sub(r'^NORTH ',  '',  address)
    address = re.sub(r' EYE ',  ' I ',  address)
    
    # strip tailing Quadrant data
    address = re.sub(r' (NE|NW|SE|SW)$',  '',  address)
    
    return address

def main_loop():

    drop_index('geodc/address')
    #set_address_mapping()
    cntr = 1
    with db_cursor() as cursor:
        #cursor = db_cursor()
        cursor.execute("""SELECT to_char(address_id, '000000000000000D')::TEXT, fulladdress,
            city, 'DC' as state, zipcode::TEXT, addrnum::TEXT, local_id::TEXT, 'SSL' as local_desc, res_type as addr_use,
            st_asgeojson(st_expand(geometry, 0.0000001)) as extent,
            st_asgeojson(geometry) as location,
            '' as neighborhood,
            '{}'::TEXT as camera,
            '{}'::TEXT as front_vect
            FROM temp.address_points""")
        result = cursor.fetchall()
        for data in result:
            address = {"id": data[0].strip(), 
                "complete_address": data[1],
                "core_address": core_address(data[1]), 
                "alt_core_address": alt_core_address(data[1]), 
                "city": data[2],
                "state": data[3],
                "zipcode": data[4],
                "address_number": int(data[5]),
                "local_id": data[6],
                "local_desc": data[7], 
                "addr_use": data[8], 
                "extentBBOX": json.loads(data[9]), 
                "location": json.loads(data[10]), 
                "neighborhood": data[11], 
                "camera": json.loads(data[12]), 
                "front_vect": json.loads(data[13])
            }
            send_address(address)
            if (cntr % 5000) == 0:
                time.sleep(3)
            cntr += 1
    pass
    
if __name__ == "__main__":
    main_loop()
    pass
