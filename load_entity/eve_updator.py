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

def parse_eve_keys():
    pass
    
def parse_eve_metadata():
    with open("metadata.json", "r") as read_file:
        data = json.load(read_file)
        with open('metadata_eve.txt', 'w') as out:

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
                    
    """         
                else:
                    container_name = item['container_type']
                    #container_key = item['container_key']
                    raw_edge = ''
                    edge_type = item['edge_type']
                    #out.write(variable_name+'|'+container_name+'|'+raw_edge+'\n')
    """