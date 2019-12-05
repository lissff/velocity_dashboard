# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from utils import utils
from entities.watermark import Watermark
from entities.var import Var
from entities.radd import RADD
from entities.model import Model
from entities.edge_container import EdgeContainer
import ssl
import json
import urllib2
from utils.graph_db import graph

class EVEUpdator(object):
    """
        update from eve API before finally migrate dashboard to EVEBuilder
    """
    def __init__(self):
        self.EDGE_SEARCH_URL = 'http://grds.paypalinc.com/evebuilder/api/metadata/raw_variables?searchType=by_definition&searchData={event},,,,,,&searchSimilar=false'
        self.EDGE_DEFINITION_URL = 'http://grds.paypalinc.com/evebuilder/api/variables/released_raw_edge'

    def get_event_raw_dependency_from_eve(self, event):
        """
        weekly fetch from evebuilder eventkey -[:ATTRIBUTE_OF] - raw relationship
                                and  set raw edge's updator, type, filter, target, etc

        """

        MAP_EVENT_VAR ="merge(v:Var{{ name: '{var}'}})   \
                        on create  merge(e:EventKey{{name: '{eventkey}' }}) \
                        merge (v)-[:ATTRIBUTE_OF] ->(e) "

        SET_EVE_PROPERTY = "match(v:Var{{ name: '{var}'}}) set v.filter = '{filter}', v.eve_type= '{type}', \
                        v.eve_key='{key}', v.target=\"{target}\", v.function='{function}' "
        
        opener = urllib2.build_opener()
        opener.addheaders.append(('Cookie', 'edge_builder=s%3A6PKujq1oXg40Qfufm012tKkAuX4oOppH.ieuPnrNPgSy5qJ1y1qhhj6CZ7eWtJu8S5mhxMy0zzy0'))
        html = opener.open(self.EDGE_SEARCH_URL.format(event=event))
        json_file = json.loads(html.read())

        var_list = json_file['data']
        
    
        for var in var_list:
            json_keys =  var.keys()
            for updator in var['updators']:
                try:
                    if updator['rollupMessages'] == event:
                        print (SET_EVE_PROPERTY.format(var=var['name'], filter= updator['filter'] , \
                        type = var['type'], key =updator['key'], target=updator['target'], function = updator['func'] ))
                        graph.cypher.execute(SET_EVE_PROPERTY.format(var=var['name'], filter= updator['filter'] , \
                        type = var['type'], key =updator['key'], target=updator['target'], function = updator['func'] ))
                        print MAP_EVENT_VAR.format(eventkey=event+'.'+updator['key'], var=var['name'])
                        graph.cypher.execute(MAP_EVENT_VAR.format(eventkey=event+'.'+updator['key'], var=var['name']))
                except:
                    pass

    def _should_process_raw_edge():
        MAP_EVENT_VAR ="match(v:Var{{ name: '{var}'}})   \
                        where v.is \
                        merge (v)-[:ATTRIBUTE_OF] ->(e) "
        pass


    def get_edge_definition(self):
        eve_file = open('./resources/eve_definition.csv', "w")
        opener = urllib2.build_opener()
        opener.addheaders.append(('Cookie', 'edge_builder=s%3A6PKujq1oXg40Qfufm012tKkAuX4oOppH.ieuPnrNPgSy5qJ1y1qhhj6CZ7eWtJu8S5mhxMy0zzy0'))
        html = opener.open(self.EDGE_DEFINITION_URL)
        json_file = json.loads(html.read())
        for item in json_file['variables']:
            variable_name = item['name']
            eve_type = item['type']
            container_type = item.get('containerInstance')
            edge_factor = item.get('dataStructureFactor')
            for updator in item['updators']:
                event_message = updator.get('rollupMessages','')
                target = updator.get('target','')
                edge_func = updator.get('func','')
                eve_key = updator.get('key','')
                filter = updator.get('filter','')
              
                eve_file.write(variable_name+','+eve_type+','+str(target)+','+edge_func+','+eve_key+','+filter+','+container_type+','+event_message+'\n')

        eve_file.close()
"""
LOAD CSV FROM  "file:////eve_definition.csv" AS row
FIELDTERMINATOR ','
WITH row
MERGE (v: Var{name: row[0]}) MERGE(ek:EventKey{name: row[7]+'.'+row[4]}) merge (v)-[:ATTRIBUTE_OF]->(ek) set v.edge_type=row[1], v.target=row[2], v.edge_func=row[3], v.eve_key=row[4], v.filter=row[5]
"""
EVEUpdator().get_edge_definition()