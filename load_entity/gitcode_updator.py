import json
import urllib2
import ssl
from datetime import date
from utils.graph_db import graph
from bs4 import BeautifulSoup
import re
from utils import utils
from entities.var import Var
from entities.radd import RADD
from utils import mappings
from entities.edge_container import EdgeContainer


class GitUpdator(object):

    def __init__(self):
        
        self.UPDATE_DT = date.today()
        self.CONTEXT = ssl._create_unverified_context()

    def run_checkpoint_mapping(self):
        ret = graph.cypher.execute('match(cp:Checkpoint) return cp.name as name order by name')

        for cp in ret:
            self.get_checkpoint_var_from_code(cp.name)

    def get_checkpoint_var_from_code(self, checkpoint):
        """
            weekly fetch checkpoint -[:CHECKPOINT_OF] - var relationship from unified-parent

        """
        if checkpoint in mappings.CODE_CHECKPOINT_MAPPING.keys():
            checkpoint = mappings.VC_CP_MAPPING[checkpoint]
        file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/"+checkpoint+"FullVariableTrack/"+checkpoint+"FullVariableTrack.vt.json"
         
        new_var_set = set()
        existing_var_set = set()
        """
        html = urllib2.urlopen(checkpoint_url, context=self.CONTEXT)
        json_file = json.loads(html.read())
        """
        try:
            with open(file_path, "r") as read_file:
                json_file = json.load(read_file)
     
            for varclass in json_file['classBasedVariables']:
                var = varclass.split('.')[0]
                new_var_set.add(var)

            for varname in json_file['configurableVariables']:
                new_var_set.add(varname)
            
            ret = graph.cypher.execute("match(v:Var)-[]-(c:Checkpoint{{name:'{checkpoint}'}}) return v.name as name order by name".format(checkpoint=checkpoint))
            for variable in ret:
                existing_var_set.add(variable.name)
            
            utils.update_var_checkpoint(checkpoint, new_var_set-existing_var_set, existing_var_set-new_var_set)  
        except:
            print 'no such file found' + checkpoint
            pass

    def get_eve_keylib_from_code(self):
        """
            weekly fetch from unified-parent keylib, and set var.is_key = True

        """
        file_path = "/Users/metang/workspace/unified-parent/unified-variables/src/main/java/com/paypal/risk/idi/library/keylib/KeyLib.java"
        with open(file_path, "r") as read_file:
            soup = BeautifulSoup(read_file, 'html.parser')
        
        var_iskey = " merge (v:Var{{name:'{varname}'}}) \
                        set v.is_key = True"
        inputset = set()
        for line in soup:
            if 'AbstractBaseVariable' in line:
                keyset = re.findall("(?<=AbstractBaseVariable)(.*)(?==)", line)
                if len(keyset) >0:                     
                    for evekey in keyset:
                        inputset.add(evekey.replace(' ',''))

        rest_data = self._remove_existing_key(inputset)
        for item in rest_data:
            print item
            graph.cypher.execute(var_iskey.format(varname=item))

    def full_parse_variable_metadata(self):
        """
            weekly parse variable_metadata.json from compiling targets from variable-metadata code
        """
        with open("/Users/metang/workspace/variable-metadata/target/classes/metadata/variable/variable_metadata.json", "r") as read_file:
            data = json.load(read_file)
            rest_data = self._remove_existing_var(data)
            for item in rest_data:
                try:
                    variable_type = item.get('type')            
                    if  variable_type == 'RADD':    
                        utils.radd_handler(item)

                    else:
                        utils.edge_handler(item)
                        
                except Exception as e:
                    print e
                    pass

    def incremental_parse_variable_metadata(self, original_file, latest_file):

        origin = open(original_file, "r")
        latest = open(latest_file, "r") 

        origin_data = generate_sign(json.load(origin))
        latest_data = generate_sign(json.load(latest))

        new_set = latest_data - origin_data
        print new_set
        for item in new_set:
            variable_name = item.split('|')[0]
            for dataset in latest:
                if variable_name == dataset['name']:
                    try:
                        variable_type = dataset.get('type')            
                        if  variable_type == 'RADD':    
                            utils.radd_handler(dataset)
                        else:
                            utils.edge_handler(dataset)                      
                    except Exception as e:
                        print e
                        pass
      
        origin.close()
        latest.close()


    def _remove_existing_var(self, dataset):
        varset = set()
        retset = []
        # exlude those existing in graph db, and with dependencies somehow
        ret = graph.cypher.execute("match(v:Var)-[]-(:EdgeContainer|Radd) return v.name as name order by name")
        #ret = graph.cypher.execute("match(v:Var) where (v)-[]-(:EventKey|Radd) or v. return v.name as name order by name")
        for variable in ret:
            varset.add(variable.name)
        for i in dataset:
            if not (i['name'] in varset):
                retset.append(i)
        return retset


    def _remove_existing_key(self, inputset):
        varset = set()
        retset = []
        # exlude those existing in graph db, and with dependencies somehow
        ret = graph.cypher.execute("match(v:Var) where v.is_key = true return v.name as name order by name")
        for variable in ret:
            varset.add(variable.name)
        retset = inputset - varset
        return retset

        
    def _only_process_edge(self, dataset):
       
        retset = []
       
        for i in dataset:
            if (i['type'] == 'READING_EDGE' ):
                retset.append(i)
        return retset






GitUpdator().full_parse_variable_metadata()
#GitUpdator().get_checkpoint_var_from_code('EvalConsumerCreditUK')
#GitUpdator().get_eve_keylib_from_code()
#GitUpdator().run_checkpoint_mapping()