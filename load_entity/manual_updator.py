import json
import urllib2
import ssl
from datetime import date
from utils.graph_db import graph
from bs4 import BeautifulSoup
import re

update_dt = date.today()
CONTEXT = ssl._create_unverified_context()

def get_checkpoint_var_from_code(checkpoint):
    """
        weekly fetch checkpoint -[:CHECKPOINT_OF] -var relationship from unified-parent

    """
    if checkpoint == 'ADDCC':
        checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/AddCCFullVariableTrack/AddCCFullVariableTrack.vt.json?token=AAAR5V6TGAWKf7qDzrfPPVUpZBxshQP-ks5dQQb9wA"
    
    elif checkpoint == 'CONSOLIDATEDFUNDING':
        checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/BREFullVariableTrack/BREFullVariableTrack.vt.json?token=AAAR5fIVLZs7QnSLXTZkAomvLWOoKew6ks5dQoEzwA"
    
    elif checkpoint == 'WITHDRAWALATTEMPT':
         checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/WithdrawalFullVariableTrack/WithdrawalFullVariableTrack.vt.json?token=AAAR5Wrfu3bwy2AYRzx7Lk_h6KO1mKYmks5dU8ElwA"

    else:
        pass
    insert_euler= "  match(c:Checkpoint{{name:'{checkpoint}'}}) \
                     merge (v:Var{{name:'{varname}'}}) \
                     set v.update_dt = '{update_dt}' \
                     create unique (c)-[co:CHECKPOINT_OF]->(v) return co"
    var_set = set()
    html = urllib2.urlopen(checkpoint_url, context=CONTEXT)
    json_file = json.loads(html.read())
    for varclass in json_file['classBasedVariables']:
        var = varclass.split('.')[0]
        var_set.add(var)

    for varname in json_file['configurableVariables']:
        var_set.add(varname)
    
    for varname in var_set:
        print insert_euler.format(checkpoint=checkpoint, varname=varname, update_dt=update_dt)
        graph.cypher.execute(insert_euler.format(checkpoint=checkpoint, varname=varname, update_dt=update_dt))

def get_eve_key_from_code():
    """
        weekly fetch from unified-parent keylib, and set var.is_key = True

    """
    key_url = "https://github.paypal.com/raw/DART/unified-parent/SH/unified-variables/src/main/java/com/paypal/risk/idi/library/keylib/KeyLib.java?token=AAAR5bN6SP0qebX2coDa8jLPMueuvmrNks5dQRwLwA"
    html = urllib2.urlopen(key_url, context=CONTEXT)
    
    soup = BeautifulSoup(html, 'html.parser')
    insert_euler= "  merge (v:Var{{name:'{varname}'}}) \
                     set v.is_key = True"
    for line in soup:
        if 'AbstractBaseVariable' in line:
            keyset = re.findall("(?<=AbstractBaseVariable)(.*)(?==)", line)
            if len(keyset) >0:                     
                for evekey in keyset:
                    varname = evekey.replace(' ','')
                    graph.cypher.execute(insert_euler.format(varname=varname))

def parse_variable_metadata():
    with open("/Users/metang/workspace/variable-metadata/target/classes/metadata/variable/variable_metadata.json", "r") as read_file:
        data = json.load(read_file)
        for item in data:
            variable_type = item['type']
            variable_name = item['name']
    
        if  variable_type == 'RADD':
            radd_name = item['table_name']
            field_name = item['field_name']
            radd_key = item['keys']
            #out.write(variable_name+'|'+)
        elif variable_type == 'READING_EDGE' and item['edge_type'] == 'Decay':
            out.write(variable_name+'\n')

        elif variable_type == 'READING_EDGE'and item['edge_type'] <> 'Decay':
            container_name = item['container_type']
            #container_key = item['container_key']
            raw_edge = item['corresponding_variable']
            edge_type = item['edge_type']
            #out.write(variable_name+'|'+container_name+'|'+raw_edge+'\n')
                    


def get_event_key_dependency_from_eve(event, table_name):
    pass
    
def get_event_raw_dependency_from_eve(event, table_name):
    """
    weekly fetch from evebuilder eventkey -[:ATTRIBUTE_OF] - raw relationship
                            and  set raw edge's updator, type, filter, target, etc

    """
    URL = 'http://grds.paypalinc.com/evebuilder/api/metadata/raw_variables?searchType=\
    by_definition&searchData={event},,,,,,&searchSimilar=false'

    MAP_EVENT_VAR ="match(v:Var{{ name: '{var}'}})   \
                    merge(e:EventKey{{name: '{eventkey}' }}) \
                    merge (v)-[:ATTRIBUTE_OF] ->(e) "

    SET_EVE_PROPERTY = "match(v:Var{{ name: '{var}'}}) set v.filter = '{filter}', v.eve_type= '{type}', \
                    v.eve_key='{key}', v.target=\"{target}\", v.function='{function}' "
    """
    opener = urllib2.build_opener()
    opener.addheaders.append(('Cookie', 'edge_builder=s%3ApfzCiY2SDpADkqCGehW2K0BpmiNihLJ7.WCLfoaRIKqJZoowcuWtavXxAPLxEBUF%2F%2BJ4dbdPJBUA'))
    html = opener.open(URL.format(event=event))
    json_file = json.loads(html.read())
    """

    

    with open("decisionresult.json", "r") as read_file:
        json_file = json.load(read_file)


        var_list = json_file['data']
        for var in var_list:
            json_keys =  var.keys()
            for updator in var['updators']:
                if updator['rollupMessages'] == event:
                    print (SET_EVE_PROPERTY.format(var=var['name'], filter= updator['filter'] , \
                    type = var['type'], key =updator['key'], target=updator['target'], function = updator['func'] ))
                    graph.cypher.execute(SET_EVE_PROPERTY.format(var=var['name'], filter= updator['filter'] , \
                    type = var['type'], key =updator['key'], target=updator['target'], function = updator['func'] ))
                    print MAP_EVENT_VAR.format(eventkey=table_name+'.'+updator['key'], var=var['name'])
                    graph.cypher.execute(MAP_EVENT_VAR.format(eventkey=table_name+'.'+updator['key'], var=var['name']))


get_checkpoint_var_from_code('WITHDRAWALATTEMPT')
#get_event_raw_dependency_from_eve('DecisionResult', 'acct_rsk_dcsn_evnt')