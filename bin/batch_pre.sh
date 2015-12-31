#ES_Index=geodc_c

curl -XDELETE 'http://localhost:9200/geodc_c/address'


curl -XPOST 'http://localhost:9200/geodc_c/address' -d '{"mappings": {"address": {"properties": {"city": {"type": "string"}, "neighborhood": {"type": "string"}, "address_number": {"type": "integer"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "zipcode": {"type": "string"}, "name_type": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}, "addr_use": {"type": "string"}}}}}'


curl -XDELETE 'http://localhost:9200/geodc_c/nbhd'


curl -XPOST 'http://localhost:9200/geodc_c/nbhd' -d '{"mappings": {"nbhd": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE 'http://localhost:9200/geodc_c/landmark'


curl -XPOST 'http://localhost:9200/geodc_c/landmark' -d '{"mappings": {"landmark": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "zipcode": {"type": "string"}, "name_type": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}, "addr_use": {"type": "string"}}}}}'


curl -XDELETE 'http://localhost:9200/geodc_c/SMC'


curl -XPOST 'http://localhost:9200/geodc_c/SMC' -d '{"mappings": {"SMC": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE 'http://localhost:9200/geodc_c/SMR'


curl -XPOST 'http://localhost:9200/geodc_c/SMR' -d '{"mappings": {"SMR": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE 'http://localhost:9200/geodc_c/market'


curl -XPOST 'http://localhost:9200/geodc_c/market' -d '{"mappings": {"market": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE 'http://localhost:9200/geodc_c/postalcode'


curl -XPOST 'http://localhost:9200/geodc_c/postalcode' -d '{"mappings": {"postalcode": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'


curl -XDELETE 'http://localhost:9200/geodc_c/quadrant'


curl -XPOST 'http://localhost:9200/geodc_c/quadrant' -d '{"mappings": {"quadrant": {"properties": {"city": {"type": "string"}, "complete_address": {"type": "string"}, "core_address": {"type": "string"}, "state": {"type": "string"}, "alt_core_address": {"type": "string"}, "super_core_address": {"type": "string"}}}}}'

