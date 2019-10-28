from datetime import datetime
import requests
import re
from entities.watermark import Watermark
from entities.var import Var
from entities.radd import RADD
from entities.eventkey import EventKey
from entities.edge_container import EdgeContainer
from entities.eventmessage import EventMessage


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
    var_entity.create_unique_relationship('DEPEND_ON', raw_edge_entity.node)

def radd_handler(field_name, table_name, radd_key, var_entity):
    """Handler method for RADD dependencies.

    Creates the RADD entities and relationship to the variable.

    Args:
        var: the variable object (from the Variable Catalog).
        var_entity: Var object corresponding to the var object.
    """

    for keys in radd_key:
        if keys:
            for key in keys:
                if key['variable'] == True:
                    key_entity = Var(name=key['name'])
                    key_entity.create_node()
                    var_entity.create_unique_relationship('USING_KEY', key_entity.node)

    create_radd_entities(table_name)
    if field_name is not None:
        radd_fields_lists = get_radd_fields(field_name)
        if len(radd_fields_lists)>0:
            for radd_name, fields in zip(table_name, radd_fields_lists):
                radd_entity = RADD(name=radd_name)
                var_entity.create_unique_relationship('QUERIES', radd_entity.node,
                                                        radd_fields=fields)



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

def is_empty(str):
    if str is None or len(str) < 1:
        return True
    else:
        return False