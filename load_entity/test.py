import syslog
import json_tools
import json
from jsondiff import diff
from utils import utils
from utils.graph_db import graph

def compare_json_diff(original_file, latest_file):
 
    origin = open(original_file, "r")
    latest = open(latest_file, "r") 

    origin_data = generate_sign(json.load(origin))
    latest_data = generate_sign(json.load(latest))

    new_set = latest_data - origin_data
    print new_set
    origin.close()
    latest.close()
    return  new_set

def fetch_retired_variable(original_file, latest_file):
    origin = open(original_file, "r")
    latest = open(latest_file, "r") 
    origin_name= get_all_variable_name(json.load(origin))
    latest_name = get_all_variable_name(json.load(latest))

    retired_set = origin_name - latest_name
    origin.close()
    latest.close()
    print retired_set
    return retired_set

def get_all_variable_name(dataset):
    name_set=set()
    for item in dataset:
        name_set.add(item['name'])
    return name_set

def generate_sign(dataset):
    sign_set=set()
    for item in dataset:
        item_type = item['type']
        if item_type == 'RADD':
            sign_set.add(item['name']+'|'+item.get('keys','radd')[0][0].get('name'))
        elif item_type == 'READING_EDGE':
            sign_set.add(item['name']+'|'+item.get('container_key','edge'))
        else:
            sign_set.add(item['name']+'|'+item.get('updated_logic','edge')[0].get('filter_expression'))
    return sign_set    

def get_radd_name():
    addback=['IDI_POS_AMT_STATS', 'IDI_UNQ_DEVICE', 'IDI_POS_MERCHANT_STATS', 'IDI_GRS_COUNTRY_RSK_FACTOR', 'IDI_MCC_TI_FRAUD_SCORE', 'IDI_POS_MERCHANT_RISK']
    with open("/Users/metang/workspace/variable-metadata/target/classes/metadata/variable/variable_metadata.json", "r") as read_file:
        data = json.load(read_file)
        radd = set()
        for item in data:
            try:
                variable_type = item.get('type')            
                if  variable_type == 'RADD' and item.get('table_name') in addback:
                     utils.radd_handler(item)
                    
            except Exception as e:
                print e
                pass
     
 
def get_diff():
    set_old_ek = set()
    set_new_ek = set()

    set_old_em = set()
    set_new_em = set()

    eve_file = open('./resources/eve_definition.csv', "r")
    for row in eve_file:
        line = row.rstrip().split(',')
        set_new_ek.add(line[7]+'.'+line[4])
        
        set_new_em.add(line[7])



    ek_cypher_query = 'match(ek:EventKey) return ek.name as name'
    em_cypher_query = 'match(ek:EventMessage) return ek.name as name'
    ek_ret = graph.cypher.execute(ek_cypher_query)
    for item in ek_ret:
        set_old_ek.add(item.name)
    em_ret = graph.cypher.execute(em_cypher_query)
    for item in em_ret:
        set_old_em.add(item.name)

    print set_old_ek - set_new_ek
    print set_old_em - set_old_em


#compare_json_diff('cp_variable_metadata.json','variable_metadata.json')

#fetch_retired_variable('cp_variable_metadata.json','variable_metadata.json')
get_diff()