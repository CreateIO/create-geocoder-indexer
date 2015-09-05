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

## elasticsearch uses lowercased names
IDXNAME = "geodc_a"
RUNLIVE = False
BATCH =  cStringIO.StringIO()
BATCH_PRE = cStringIO.StringIO()

PORT=int(os.environ.get('PORT', 9200))

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

#Set DB connection info
logger.info('writing to database "%s" in instance "%s"', DB_NAME, DB_INSTANCE)
if DB_INSTANCE != 'test':
    DB_CONNECTION_STRING = 'host=%s.cvwdsktow3o7.us-east-1.rds.amazonaws.com dbname=%s user=%s password=%s' % (DB_INSTANCE, DB_NAME, DB_USER, DB_PASS)
else:
    DB_CONNECTION_STRING = 'host=%s dbname=%s user=%s password=%s port=%s' % (DBHOST, DB_NAME, DB_USER, DB_PASS, DB_PORT)
# print the connection string we will use to connect
print "Connecting to database\n ->%s" % (DB_CONNECTION_STRING)

#curl -XPOST 'http://localhost:9200/_aliases' -d '
#{
#    "actions" : [
#        { "add" : { "index" : "test1", "alias" : "alias1" } },
#        { "add" : { "index" : "test2", "alias" : "alias1" } }
#    ]
#}'


#curl -XPOST 'http://localhost:9200/_aliases' -d '
#{
#    "actions" : [
#        { "remove" : { "index" : "test1", "alias" : "alias1" } },
#        { "add" : { "index" : "test1", "alias" : "alias2" } }
#    ]
#}'

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

def send_command_live(cmd,  idx,  typ):
    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:%d/%s/%s' % (PORT, idx,  typ)

    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    if (cmd != None):
        c.setopt(pycurl.CUSTOMREQUEST, cmd)
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
        pass
    except:
        e = sys.exc_info()[0]
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()

    pass

def send_command_batch(cmd,  idx,  typ):
    BATCH_PRE.write("""
curl -X%s 'http://localhost:9200/%s/%s'\n
""" % (cmd, idx, typ))
    pass

def send_command(cmd,  idx,  typ):
    if (RUNLIVE == True):
        send_command_live(cmd,  idx,  typ)
    else:
        send_command_batch(cmd,  idx,  typ)

    pass

def send_action_live(idx,  typ,  data):
    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:%d/%s/%s' % (PORT, idx, typ)
    data = json.dumps(data)
    logger.debug(data)

    c = pycurl.Curl()
    c.setopt(pycurl.URL, github_url)
    c.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, data)
    c.setopt(c.WRITEFUNCTION, response.write)
    try:
        c.perform()
        pass
    except:
        e = sys.exc_info()[0]
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()
    pass

def send_action_batch(idx,  typ,  data):
    BATCH_PRE.write("""
curl -XPOST 'http://localhost:9200/%s/%s' -d '%s'\n
""" % (idx, typ,  json.dumps(data) ))
    pass

def send_action(idx, typ, data):    
    if (RUNLIVE == True):
        send_action_live
    else:
        send_action_batch(idx, typ, data)
    pass

def drop_index(idx,  index):

    if index == None or index == '':
        return "Failed"

    send_command("DELETE", idx,  index)
    pass

def send_mapping(address,  typemap):

    send_action(IDXNAME, typemap,  address)
    pass

def send_address_live(address, indtyp):    

    response = cStringIO.StringIO()
    github_url = 'http://127.0.0.1:%d/%s/%s/' % (PORT, IDXNAME,  indtyp) + str(address["id"])
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
        pass
    except:
        e = c.errstr()
        logger.debug("error %s" % (e))
        c.reset()

    logger.debug(response.getvalue())
    response.close()    
    pass

def send_address_batch(address,  indtyp):        
    activity = {"index": { "_index": IDXNAME,  "_type": indtyp, "_id": address['id']}}
    BATCH.write(json.dumps(activity) + "\n");
    BATCH.write(json.dumps(address) + "\n")
    pass

def send_address(address,  indtyp):
    if RUNLIVE == True:
        send_address_live(address, indtyp)
    else:
        send_address_batch(address,  indtyp)

    pass

def set_address_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "address_number": { "type": "integer" },
                "city": { "type": "string" },
                "state": { "type": "string" },
                "zipcode": { "type": "string" },
                "neighborhood": { "type": "string" },
                "addr_use": { "type": "string" },
                "name_type": {"type": "string" }
                }
            }
        }
    }
    ''' % (mapname)
    mapping = json.loads(map_str)

    send_mapping(mapping,  mapname)
    return mapping

def set_landmark_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" },
                "zipcode": { "type": "string" },
                "addr_use": { "type": "string" },
                "name_type": {"type": "string" }
                }
            }
        }
    } ''' % (mapname)

    mapping = json.loads(map_str)

    send_mapping(mapping,  mapname)
    return mapping

def set_neighborhood_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def set_submarket_C_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },                
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def set_submarket_R_mapping(mapname):
    map_str = '''{ "mappings": {
        "%s": {
            "properties": {
                "complete_address": { "type": "string" },
                "core_address": { "type": "string" },
                "super_core_address": { "type": "string" },
                "alt_core_address": { "type": "string" },                
                "city": { "type": "string" },
                "state": { "type": "string" }
                }
            }
        }
    }'''    % (mapname)
    mapping = json.loads(map_str)
    send_mapping(mapping,  mapname)
    return mapping

def number_cardinal(address):
    # convert from first to 1st and visa versa
    address = re.sub(r' FIRST ',  ' 1ST ',  address)
    address = re.sub(r' SECOND ',  ' 2ND ',  address)
    address = re.sub(r' THIRD ',  ' 3RD ',  address)
    address = re.sub(r' FOURTH ',  ' 4TH ',  address)
    address = re.sub(r' FIFTH ',  ' 5TH ',  address)
    address = re.sub(r' SIXTH ',  ' 6TH ',  address)
    address = re.sub(r' SEVENTH ',  ' 7TH ',  address)
    address = re.sub(r' EIGHTH ',  ' 8TH ',  address)
    address = re.sub(r' NINTH ',  ' 9TH ',  address)
    address = re.sub(r' TENTH ',  ' 10TH ',  address)
    address = re.sub(r' ELEVENTH ',  ' 11TH ',  address)
    address = re.sub(r' TWELFTH ',  ' 12TH ',  address)
    address = re.sub(r' THIRTEENTH ',  ' 13TH ',  address)
    address = re.sub(r' FOURTEENTH ',  ' 14TH ',  address)

    return address

def cardinal_number(address):
    # convert from first to 1st and visa versa
    address = re.sub(r' 1ST ', ' FIRST ', address)
    address = re.sub(r' 2ND ',  ' SECOND ',  address)
    address = re.sub(r' 3RD ',  ' THIRD ',  address)
    address = re.sub(r' 4TH ',  ' FOURTH ',  address)
    address = re.sub(r' 5TH ',  ' FIFTH ',  address)
    address = re.sub(r' 6TH ',  ' SIXTH ',  address)
    address = re.sub(r' 7TH ',  ' SEVENTH ',  address)
    address = re.sub(r' 8TH ',  ' EIGHTH ',  address)
    address = re.sub(r' 9TH ',  ' NINTH ',  address)
    address = re.sub(r' 10TH ',  ' TENTH ',  address)
    address = re.sub(r' 11TH ',  ' ELEVENTH ',  address)
    address = re.sub(r' 12TH ',  ' TWELFTH ',  address)
    address = re.sub(r' 13TH ',  ' THIRTEENTH ',  address)
    address = re.sub(r' 14TH ',  ' FOURTEENTH ',  address)

    return address

def strip_type(address):
    address = re.sub(r' STREET',  ' ',  address)
    address = re.sub(r' COURT',  ' ',  address)
    address = re.sub(r' CIRCLE',  ' ',  address)

    address = re.sub(r' AVENUE',  ' ',  address)
    address = re.sub(r' ROAD',  ' ',  address)
    address = re.sub(r' DRIVE',  ' ',  address)
    address = re.sub(r' PLACE',  ' ',  address)
    address = re.sub(r' PLAZA',  ' ',  address)
    address = re.sub(r' TERRACE',  ' ',  address)
    address = re.sub(r' TRAIL',  ' ',  address)
    address = re.sub(r' GREEN',  ' ',  address)
    address = re.sub(r' WAY',  ' ',  address)
    address = re.sub(r' WALK',  ' ',  address)
    address = re.sub(r' ALLEY',  ' ',  address)
    address = re.sub(r' BOULEVARD',  ' ',  address)
    address = re.sub(r' CRESCENT',  ' ',  address)
    address = re.sub(r' MEWS',  ' ',  address)
    address = re.sub(r' LANE',  ' ',  address)
    address = re.sub(r' PARKWAY',  ' ',  address)
    address = re.sub(r' PARK',  ' ',  address)
    address = re.sub(r' EXPRESSWAY',  ' ',  address)
    address = re.sub(r' ROW',  ' ',  address)
    address = re.sub(r' FREEWAY',  ' ',  address)
    address = re.sub(r' BRIDGE',  ' ',  address)

    return address

def abbr_Type(address):    
    # abbreviate address street types with the USPS preferred abbreviation
    address = re.sub(r' STREET',  ' ST',  address)
    address = re.sub(r' COURT',  ' CT',  address)
    address = re.sub(r' CIRCLE',  ' CIR CR',  address)
    address = re.sub(r' AVENUE',  ' AVE AV',  address)
    address = re.sub(r' ROAD',  ' RD',  address)
    address = re.sub(r' DRIVE',  ' DR',  address)
    address = re.sub(r' PLACE',  ' PL PLC',  address)
    address = re.sub(r' PLAZA',  ' PLZ',  address)
    address = re.sub(r' TERRACE',  ' TERR TR',  address)
    address = re.sub(r' TRAIL',  ' TRL TR',  address)
    address = re.sub(r' GREEN',  ' GRN GR',  address)
    address = re.sub(r' WAY',  ' WY',  address)
    address = re.sub(r' WALK',  ' WLK',  address)
    address = re.sub(r' ALLEY',  ' ALY',  address)
    address = re.sub(r' BOULEVARD',  ' BLVD BL',  address)
    address = re.sub(r' CRESCENT',  ' CRES',  address)
    address = re.sub(r' MEWS',  ' MWS',  address)
    address = re.sub(r' LANE',  ' LA LN',  address)
    address = re.sub(r' PARKWAY',  ' PKWY',  address)
    address = re.sub(r' PARK',  ' PK',  address)
    address = re.sub(r' EXPRESSWAY',  ' EXPY EY',  address)
    address = re.sub(r' ROW',  ' RW',  address)
    address = re.sub(r' FREEWAY',  ' FWY FY',  address)
    address = re.sub(r' BRIDGE',  ' BR',  address)

    return address

def super_core_address(address):
    # this is an attempt to remove all words with a greater frequency than 10%
    address = strip_type(address)
    address = re.sub(r' (NE|NW|SE|SW)$',  ' ',  address)
    address = re.sub(r'  ',  ' ',  address)

    return address

def core_address(address):
    # strip leading  Directionality
    # IGNORE FOR DC at this time
    #address = re.sub(r'^NORTH ',  '',  address)

    # strip tailing Quadrant data
    address = re.sub(r' (NE|NW|SE|SW)$',  ' ',  address)

    #convert Streets to Types
    address= abbr_Type(address)

    return address

def alt_address(address, force): 
    # custom DC rules go here
    new_address = re.sub(r' EYE ',  ' I ',  address)
    
    # attempt to convert 11th => eleventh
    test_address = number_cardinal(new_address)
    if (test_address == new_address):
        test_address = cardinal_number(new_address)

    # unless we force it - only return a changed address
    if (force or test_address != address):
        return test_address
    else:
        return ""

def alt_core_address(address):
    # strip leading  Directionality
    #address = re.sub(r'^NORTH ',  '',  address)
    address = re.sub(r' EYE ',  ' I ',  address)

    # strip tailing Quadrant data
    address = re.sub(r' (NE|NW|SE|SW)$',  '',  address)

    return address

def index_landmarks(prm):

    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, 'landmark')
        set_landmark_mapping('landmark')

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT addralias_id::TEXT, aliasname as name,
            city, state, zipcode,
            'DCMAR'::TEXT as domain, 0 as normative,  
            status,
            aliastype,
            ssl,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(a.geometry) as location,
            a.fulladdress as proper_address
            FROM
                temp.location_alias a""")
        result = cursor.fetchall()
        for data in result:
            address = {"id": data[0].strip(),
                "proper_address": data[11] + ", " + data[1],            
                "complete_address": data[1],
                "core_address": core_address(data[1]), 
                "super_core_address": super_core_address(data[1]), 
                "alt_core_address": alt_address(super_core_address(data[1]), False), 
                "city": data[2],
                "state": data[3],
                "zipcode": data[4], 
                "domain": data[5],
                "normative": int(data[6]),
                "status": data[7], 
                "aliastype": data[8], 
                "local_id": data[9], 
                "extentBBOX": json.loads(data[10]), 
                "location": json.loads(data[10]), 
                "camera": {}
            }
            send_address(address,  'landmark')
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def index_neighborhoods(prm):

    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME,'neighborhood')
        set_neighborhood_mapping('neighborhood')

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT 'NBHD:' || nbhd::TEXT, a.descriptio as name,
            'WASHINGTON' as city,
            'DC' as state,
            'DCZ'::TEXT as domain, 0 as normative,  
            'G' as class,
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location
            FROM
                temp.nbhd a""")
        result = cursor.fetchall()
        for data in result:
            address = {"id": data[0].strip(), 
                "proper_address": data[1],            
                "complete_address": data[1],
                "city": data[2],
                "state": data[3],
                "domain": data[4],
                "normative": int(data[5]),
                "class": data[6],
                "extentBBOX": json.loads(data[7]), 
                "location": json.loads(data[8]), 
                "neighborhood": data[1], 
                "camera": {}
            }
            send_address(address,  'neighborhood')
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def submit_address(data):
    address = {"id": data[0].strip(), 
        "proper_address": data[14], 
        "complete_address": data[1],
        "core_address": core_address(data[1]), 
        "super_core_address": super_core_address(data[1]), 
        "alt_core_address": alt_address(super_core_address(data[1]), False),
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
    send_address(address,  'address')


def index_addresses(prm):

    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME, 'address')
        set_address_mapping('address')

    cntr = 1
    with db_cursor() as cursor:
        #cursor.exeute('''CREATE TEMPORARY TABLE''')

        cursor.execute("""DROP TABLE IF EXISTS address_list_temp""")
        cursor.execute("""CREATE TABLE address_list_temp AS (SELECT to_char(a.address_id, '000000000000000D')::TEXT as indexable_id,
            a.fulladdress,
            a.city, 'DC' as state, a.zipcode::TEXT, a.addrnum::TEXT, a.local_id::TEXT, 'SSL' as local_desc, a.res_type as addr_use,
            st_asgeojson(st_expand(a.geometry, 0.0000001)) as extent,
            st_asgeojson(a.geometry) as location,
            n.descriptio as neighborhood,
            '{}'::TEXT as camera,
            coalesce(p.front_vect,'{}')::TEXT as front_vect,
            a.fulladdress as proper_address
            FROM (temp.address_points a LEFT OUTER JOIN
                temp.nbhd n ON (st_intersects(a.geometry, n.geometry)))  LEFT OUTER JOIN
                development.properties p ON (p.local_id = a.local_id))""")

        cursor.execute("""SELECT * FROM address_list_temp""")
        result = cursor.fetchall()
        for data in result:
            submit_address(data)
            test_alt = alt_address(data[1], True)
            if (test_alt != data[1]):
                newData = []
                for d in data:
                    newData.append(d)
                newData[0] = data[0] +"_a"
                # rewrite the proper address
                newData[1] = test_alt
                submit_address(newData)

            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1
    pass

def index_submarket_commercial(prm):

    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME,'SMC')
        set_submarket_C_mapping('SMC')

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT ogc_fid::TEXT, replace(a.name, '_', ' ') as name,
            'WASHINGTON' as city,
            'DC' as state,
            'create.io'::TEXT as domain, 0 as normative,  
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location
            FROM
                temp.submarket_commercial_nbhd a""")
        result = cursor.fetchall()
        for data in result:
            address = {"id": data[0].strip(),             
                "proper_address": data[1],
                "complete_address": data[1],
                "core_address": core_address(data[1]), 
                "super_core_address": super_core_address(data[1]), 
                "alt_core_address": alt_address(super_core_address(data[1]), True),         
                "city": data[2],
                "state": data[3],
                "domain": data[4],
                "normative": int(data[5]),
                "extentBBOX": json.loads(data[6]), 
                "location": json.loads(data[7]), 
                "camera": {}
            }
            send_address(address,  'SMC')
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def index_submarket_residential(prm):

    if 'reset' in prm and prm and prm['reset'] == True:
        drop_index(IDXNAME,'SMR')
        set_submarket_R_mapping('SMR')

    cntr = 1
    with db_cursor() as cursor:
        cursor.execute("""SELECT objectid::TEXT, name,
            'WASHINGTON' as city,
            'DC' as state,
            'create.io'::TEXT as domain, 0 as normative,  
            st_asgeojson(st_expand(a.geometry, 0.000001)) as extent,
            st_asgeojson(st_pointonsurface(a.geometry)) as location
            FROM
                temp.submarket_residential_nbhd a""")
        result = cursor.fetchall()
        for data in result:
            address = {"id": data[0].strip(), 
                "proper_address": data[1],
                "complete_address": data[1],
                "core_address": core_address(data[1]), 
                "super_core_address": super_core_address(data[1]), 
                "alt_core_address": alt_address(super_core_address(data[1]), True),                          
                "city": data[2],
                "state": data[3],
                "domain": data[4],
                "normative": int(data[5]),
                "extentBBOX": json.loads(data[6]), 
                "location": json.loads(data[7]), 
                "camera": {}
            }
            send_address(address,  'SMR')
            if (cntr % 5000) == 0:
                time.sleep(0)
            cntr += 1

    pass

def main_loop():
    index_addresses({"reset": True})
    index_neighborhoods({"reset": True})
    index_submarket_commercial({"reset": True})
    index_submarket_residential({"reset": True})

    index_landmarks({"reset": True})

    if (RUNLIVE == False):
        BATCH_PRE.reset()
        with  open("batch_pre.json",  "wb+") as bfile:
            bfile.write(BATCH_PRE.getvalue())
        BATCH_PRE.close()

        BATCH.reset()
        with  open("batch.json",  "wb+") as bfile:
            bfile.write(BATCH.getvalue())
        BATCH.close()
    pass

if __name__ == "__main__":
    main_loop()
    pass
