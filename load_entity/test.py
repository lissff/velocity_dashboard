import syslog
import json_tools
import json
from jsondiff import diff


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


compare_json_diff('cp_variable_metadata.json','variable_metadata.json')

fetch_retired_variable('cp_variable_metadata.json','variable_metadata.json')