import json
import re
from graph_db import graph

def get_key_diff():
    eve_keys = set()
    var_keys = set()
    with open("eve_keys.txt", "r") as read_file:
        with open('eve_keys_final.txt', 'w') as out:  
            for line in read_file:
                if 'AbstractBaseVariable' in line:
                    evekey = re.findall("(?<=AbstractBaseVariable)(.*)(?==)", line)
                    if len(evekey) >0:       
                        print evekey[0].replace(' ','')
                        eve_keys.add(evekey[0].replace(' ',''))
                        out.write(evekey[0].replace(' ','')+'\n')

    with open("key_variable.txt", "r") as read_file: 
        for line in read_file:       
            var_keys.add(line.rstrip())
    
    diff = eve_keys - var_keys
    print diff

def load_event_key():
    rollupkey_set = set()
    statement = (
           " MATCH(e:EventMessage {{eve_msg_id: '{message_id}'}}) return e.name "
        )
    with open("rollupkeys.json", "r") as read_file:
        with open('rollup_key.csv', 'w') as out:  
            for line in read_file:
                line = json.loads(line)
                rollup_key = line['name']
                message_id = line['messageId']
                print rollup_key, message_id
                rollupkey_set.add(rollup_key)
                
         
                message_name = graph.cypher.execute(statement.format(message_id=message_id)).one
                print message_name
                out.write(message_name+','+rollup_key)
                out.write('\n')
   # print rollupkey_set
                

def parse_ontologies():     
    with open("ontologies.json", "r") as read_file:
        with open('onto_event_key.csv', 'w') as out:  
            for line in read_file:
                line = json.loads(line)
                ontology = line['name']
                for item in line['event_mapping']:
                    if item['data_type'] == 'event':
                        message = item['data_name']
                        for role in item['roles']:
                            key = role['mapping']
                            out.write(ontology+','+message+'_'+key)
                            out.write('\n')

def load_event():
    statement = (
           " MERGE(e:EventMessage {{name: '{message}'}}) set e.eve_msg_id = '{message_id}' return e"
        )
    with open("messages.json", "r") as read_file:
        for line in read_file:
            line = json.loads(line)
            message = line['name']
            message_id = line['_id']['$oid']
            print message, message_id
            print statement.format(message=message, message_id=message_id)
            graph.cypher.execute(statement.format(message=message, message_id=message_id))

def get_onto_event_diff():
    with open('rollup_key.csv', 'r') as read_file:
        with open('key_ontology.csv', 'w') as out:
            for line in read_file:
                query_key = line.replace(',','_').rstrip().replace(' ','')
                ontology = ''
                with open('onto_event_key.csv', 'r') as map_read_file:
                    for map_line in map_read_file:
                    
                        if query_key in map_line:
                            ontology = map_line.split(',')[0].rstrip()
                            print query_key, ontology
                
                out.write(line.rstrip()+','+ontology)
                out.write('\n')

    
get_onto_event_diff()
