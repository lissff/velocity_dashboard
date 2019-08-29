
import json
import urllib2
import ssl
from graph_db import graph

CONTEXT = ssl._create_unverified_context()

URL = 'http://grds.paypalinc.com/evebuilder/api/metadata/raw_variables?searchType=\
by_definition&searchData={event},{key},,,,,&searchSimilar=false'

MAP_EVENT_VAR = "match(e:EventKey{{name: '{eventkey}' }}) \
                 match(v:Var{{ name: '{var}'}})           \
                 merge (v)-[:ATTRIBUTE_OF] ->(e) "

SET_EVE_PROPERTY = "match(v:Var{{ name: '{var}'}}) set v.filter = '{filter}', v.eve_type= '{type}', v.eve_key='{key}', v.target='{target}', v.function='{function}' "
def parse_var_event(event, key, table_name):
    print URL.format(event=event, key=key)
    opener = urllib2.build_opener()
    opener.addheaders.append(('Cookie', 'edge_builder=s%3A_duW0dyMu00pX0bA2vRtFxLZEOLp2Xlx.31O43pfFQJn8m0PdmI9swn9l5IfvSs2e%2FZFgYImGZp8'))
    html = opener.open(URL.format(event=event, key=key))
    print html
    json_file = json.loads(html.read())
    var_list = json_file['data']
    for var in var_list:
        json_keys =  var.keys()
        for updator in var['updators']:
            if updator['rollupMessages'] == event:
                #print var['name'], var['type'], updator['filter'], updator['key'], updator['target'],updator['func']
                #print var['type']
                graph.cypher.execute(SET_EVE_PROPERTY.format(var=var['name'], filter= updator['filter'] , \
                type = var['type'], key =updator['key'], target=updator['target'], function = updator['func'] ))
                #graph.cypher.execute(MAP_EVENT_VAR.format(eventkey=table_name+'.'+updator['key'], var=var['name']))

parse_var_event('CreditCardCreated','' ,'acct_cc_event')