import json
import json
import urllib2
import ssl

def transform_variable_metadata(filepath):
    """
        weekly parse variable_metadata.json from compiling targets from variable-metadata code
    """
    with open("/Users/metang/workspace/variable-metadata/target/classes/metadata/variable/variable_metadata.json", "r") as read_file:
        with open(filepath, 'w') as out:
            data = json.load(read_file)
            for line in data:
                out.write('{"index":{}}')
                out.write('\n')
                out.write(json.dumps(line).rstrip())
                out.write('\n')
            
def download_vc_rtcs():
    CONTEXT = ssl._create_unverified_context()
    URL = 'http://10.176.1.109:3000/api/variables'  + '/?token=' + 'a1f98939c5b8cea62b099e5fb13cf5b7' + '&select=all&service=' + 'rtcs'
    html = urllib2.urlopen(URL, context=CONTEXT)
    variables = json.loads(html.read())
    with open('rtcs_vc.txt', 'w') as out:
        for line in variables:
            out.write('{"index":{}}')
            out.write('\n')
            line.pop('_id')
            line.pop('last_update')
            out.write(json.dumps(line).rstrip())
            # remove "_id": "550291eaf33b2b56ad70c483"}

            out.write('\n')

def transform_neo4j(filepath):
    """
        weekly parse variable_metadata.json from compiling targets from variable-metadata code
    """
    with open("query.json", "r") as read_file:
        with open(filepath, 'w') as out:
            
            for line in read_file:
                out.write('{"index":{}}')
                out.write('\n')
                out.write(line)

            out.write('\n')

transform_neo4j('neo4j.json')
#transform_variable_metadata('vc_edge_raw.txt')
#transform_variable_metadata('rtcs_vc.txt')
