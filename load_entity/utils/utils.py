from datetime import datetime
import requests
import re
from entities.watermark import Watermark
from entities.var import Var
from entities.radd import RADD
from entities.eventkey import EventKey
from entities.edge_container import EdgeContainer
from entities.eventmessage import EventMessage
from graph_db import graph
import mappings



def get_string(iterator, literal_terminator='"'):
    """Gets a string enclosed in a literal terminator.

    The first string in the iterator is already part of the returned string.

    Args:
        iterator: A string iterator.
        literal_terminator: defines the string ending character (defaults to ").

    Returns:
        The enclosed string.
    """
    in_string_literal = True
    str_ = ''
    while in_string_literal:
        token = iterator.next()
        if token in literal_terminator:
            in_string_literal = False
        else:
            str_ += token
    return str_


def date_str_to_datetime(date_str, delimiter='-'):
    """Converts date string to datetime object

    Args:
        date_str: A string represents a date, delimited by the delimiter parameter
            e.g. 1970-01-01 (delimiter is '-')
        delimiter: Delimites the different parts of the date string (defaults to '-')

    Returns:
        The datetime object.
    """
    return datetime(*map(int, date_str.split(delimiter)))




def datetime_to_epoch(dt_object):
    """Converts datetime object to seconds since epoch.

    Args:
        dt_object: A datetime object.

    Returns:
        Seconds since epoch (integer).
    """
    return int((dt_object - datetime(1970, 1, 1)).total_seconds())


def api_request(address):
    """Makes a get request.

    Args:
        address: the url address for the api get request.

    Returns:
        The reponse object.

    Raises:
        HTTPError
    """
    r = requests.get(address)
    if r.status_code != 200:
        error_message = "bad status code: {0}".format(r.status_code)
        raise requests.exceptions.HTTPError(error_message)
    return r

def edge_handler(item):
    variable_type = item.get('type')
    """
    if variable_type == 'WRITING_EDGE':
        aggregation_type = item.get('aggregation_type', 'Empty')
        container_name = item.get('container_type', 'Empty')
        writing_edge_type = item.get('value_type', 'Empty')
        message_name = map_eventmessage(item['updated_logic'][0]['triggered_event'])
        event_key = map_eventkey(item['updated_logic'][0]['key_expression'])

        if item['edge_type'] == 'Decay' and '_dk' not in variable_name: # the EVE rewrite way,  we need to flattern logical decay writing edge case in EVE
            for aggr_type in aggregation_type:
                for factor in item['factors']:
                    variable_name = variable_name+'_'+lower(aggr_type)+'_dk_'+factor
                    var_entity = create_raw_edge_entity(variable_name, aggr_type, writing_edge_type)
                    writing_edge_handler(container_name, var_entity, message_name, event_key )
        else: # normal way
                var_entity = create_raw_edge_entity(variable_name, aggregation_type, writing_edge_type)
                writing_edge_handler(container_name, var_entity, message_name, event_key )

    elif variable_type == 'READING_EDGE':
    """
    if variable_type == 'READING_EDGE':
        aggregation_type = item.get('aggregation_type', 'Empty')
        container_name = item.get('container_type', 'Empty')
        container_key = item.get('container_key', 'Empty')
        edge_type = item.get('edge_type', 'Empty')
        raw_edge = item.get('corresponding_variable', 'Empty')
        if edge_type == 'Decay':
            var_entity = create_raw_edge_entity(variable_name, aggregation_type, edge_type)
            reading_edge_handler(container_name, container_key, variable_name , var_entity)

        elif edge_type == 'TSNC' and 'tsnc' not in variable_name: # EVE way
            
            raw_edge = item['corresponding_variable']+'_last_upd_ts'
            var_entity = create_reading_edge_entity(container_key, aggregation_type, variable_name, edge_type)
            reading_edge_handler(container_name, container_key, raw_edge , var_entity)

        else:
            var_entity = create_reading_edge_entity(container_key, aggregation_type, variable_name, edge_type)
            reading_edge_handler(container_name, container_key, raw_edge , var_entity)

def writing_edge_handler(edge_container, var_entity, event_message, event_key):
    """Handler method for Edge Container dependencies.

    Creates the Edge Container entities and relationship to the variable.

    Args:
        var: the variable object (from the Variable Catalog).
        var_entity: Var object corresponding to the var object.
    """
    if is_empty(edge_container):
        return      
    edge_entity = EdgeContainer(name=edge_container)
    edge_entity.create_node()
    var_entity.create_unique_relationship('ATTRIBUTE_OF', edge_entity.node)
    

    if is_empty(event_key):
        return   
    eventkey = EventKey(name=event_key)
    eventkey.create_node()
    var_entity.create_unique_relationship('ATTRIBUTE_OF', eventkey.node)

    eventmsg = EventMessage(name=event_message)
    eventmsg.create_node()
    print event_message, event_key
    eventmsg.create_unique_relationship('HAVE_KEY', eventkey.node)



def reading_edge_handler(edge_container, edge_key, raw_edge, var_entity):
    """Handler method for Edge Container dependencies.

    Creates the Edge Container entities and relationship to the variable.

    Args:
        var: the variable object (from the Variable Catalog).
        var_entity: Var object corresponding to the var object.
    """
    if is_empty(edge_container):
        return      
    edge_entity = EdgeContainer(name=edge_container)
    edge_entity.create_node()
    var_entity.create_unique_relationship('ATTRIBUTE_OF', edge_entity.node)

    if is_empty(edge_key):
        return
    key_entity = Var(name=edge_key)
    key_entity.create_node()
    var_entity.create_unique_relationship('USING_KEY', key_entity.node)

    if is_empty(raw_edge):
        return 
    raw_edge_entity = Var(name=raw_edge, is_raw_edge=True)
    raw_edge_entity.create_node()
    var_entity.create_unique_relationship('DERIVE_FROM', raw_edge_entity.node)

def radd_handler(item):
    """Handler method for RADD dependencies.

    Creates the RADD entities and relationship to the variable.

    Args:
        var: the variable object (from the Variable Catalog).
        var_entity: Var object corresponding to the var object.
    """ 
    variable_type = item.get('type')
    variable_name = item.get('name')
    table_name = item['table_name']
    field_name = item['field_name']
    radd_key = item['keys']
    var_entity = create_radd_var(variable_type, variable_name)
    
    for keys in radd_key:
        if keys:
            for item in keys:
                if 'variable' in item: 
                    print 'keyname  '+item['name']
                    key_entity = Var(name=item['name'])
                    key_entity.create_node()
                   
                    var_entity.create_unique_relationship('USING_KEY', key_entity.node)

    if is_empty(table_name):
        pass
    radd_entity = create_radd_entities(table_name)

    
    var_entity.create_unique_relationship('QUERIES', radd_entity.node)


def create_radd_var(variable_type, variable_name, iseve):
    """Creates the variable entity and the remote node in the datastore.

    Args:
        var: a dictionary with the variable data.
    Returns:
        The newly created entity.
    """
    try:

        var_entity = Var(name=variable_name,
                         is_eve_radd=True,
                        type='RADD')
        var_entity.create_node()
        return var_entity
    except ValueError:
        pass
def get_radd_fields(fields):
    """Processes the RADD fields string from the Variable Catalog.

    The RADD fields correspond to the RADDs list in the same variable object.
    Fields used per RADD are seperated by a comma (','). Per RADD the fields are
        seperated by tilde ('~'). For example:
        'field_1~field_2,other_field' ==> [['field_1', 'field_2'], ['other_field']]

    Args:
        fields: A string represents the used RADDs fields.
    Returns:
        A list of lists of RADD fields (strings).
    """
    # get RADD fields per RADD

    radd_fields = fields.split(',')
    # if more than one field is used per RADD, '~' is used as a delimiter
    # split by '~' and return list of lists
    radd_fields_lists = [get_unique_fields(f) for f in radd_fields]
    return radd_fields_lists

def get_unique_fields(fields):
    """Remove fields duplicated

    Args:
        fields: A string of fields, seperated by tilde ('~').
    Returns:
        A list of unique fields (strings).
    """
    # remove None values from list and get unique values (set)

    unique_fileds = set([f for f in fields.split('~') if f is not None])
    return list(unique_fileds)
    
def create_radd_entities(radd_name):
    """Creates RADD entities and their corresponding nodes in the datastore.

    Args:
        radds_list: a list of Model names.
    """
    
    radd_entity = RADD(name=radd_name)
    radd_entity.create_node()
    return radd_entity

def is_empty(str):
    if str is None or len(str) < 1:
        return True
    else:
        return False

def map_eventmessage(message):


    if message in mappings.EVE_EVENT_MAPPING.keys():
        event_message = mappings.EVE_EVENT_MAPPING[message]
        return event_message

def update_var_checkpoint(checkpoint, newset, removeset):

    add_var_checkpoint= " match(c:Checkpoint{{name:'{checkpoint}'}}) \
                    merge (v:Var{{name:'{varname}'}}) \
                    create unique (c)-[co:CHECKPOINT_OF]->(v) return co"

    remove_var_checkpoint = " match(c:Checkpoint{{name:'{checkpoint}'}})-[rel]-(v:Var{{name:'{varname}'}}) \
                    delete rel"
    for varname in newset:
        if varname:
            try:
                print 'new variable ' + varname
                graph.cypher.execute(add_var_checkpoint.format(checkpoint=checkpoint, varname=varname))
            except Exception as e:
                print e
                pass
                    
    for varname in removeset:
        if varname:
            print 'retired variable '+ varname +' from ' + checkpoint
            graph.cypher.execute(remove_var_checkpoint.format(checkpoint=checkpoint, varname=varname))
        
def map_eventkey(eventkey):
    message_name = eventkey.split('.')[1]
    if message_name in mappings.EVE_EVENT_MAPPING.keys():
        event_key = eventkey.replace(message_name, mappings.EVE_EVENT_MAPPING[message_name])
        return event_key


def create_raw_edge_entity(variable_name, aggregation_type, edge_type):

    var_entity = Var(name=variable_name ,
                        type='EDGE',
                        aggregation_type = aggregation_type,
                        edge_type = edge_type,
                        is_raw_edge = True)
    var_entity.create_node()
    return var_entity

def create_reading_edge_entity(container_key, aggregation_type, variable_name, edge_type):

    var_entity = Var(name=variable_name,
                        type='EDGE',
                        aggregation_type = aggregation_type,
                        container_key= container_key,
                        edge_type = edge_type,
                    )
    var_entity.create_node()
    return var_entity






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