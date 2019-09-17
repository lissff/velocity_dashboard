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

from entities.edge_container import EdgeContainer


class GitUpdator(object):
    def __init__(self):
        
        self.UPDATE_DT = date.today()
        self.CONTEXT = ssl._create_unverified_context()
        self.CHECKPOINTS = ['ADDCC' , 'CONSOLIDATEDFUNDING' , 'WITHDRAWALATTEMPT']

        self.EVENTS = ['CreditCardCreated', 'CreditCardChanged', 'CCAuth', 'PaymentAttempt', \
                'DecisionResult', 'RiskWithdrawalAttemptDecision', 'PaymentDebit' \
                'PaymentDebitNewDO', 'PaymentAttemptNewDO', 'PaymentAttemptVO']

        
    def run_checkpoint_mapping(self):
        for cp in self.CHECKPOINTS:
            self.get_checkpoint_var_from_code(cp)

    def get_checkpoint_var_from_code(self, checkpoint):
        """
            weekly fetch checkpoint -[:CHECKPOINT_OF] - var relationship from unified-parent

        """
        if checkpoint == 'ADDCC':
            checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/AddCCFullVariableTrack/AddCCFullVariableTrack.vt.json?token=TfJMtn7nTXEDuwnnzB64ViLmIHya_SPZHcrQegk_0lKNLBcZ"
            file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/AddCCFullVariableTrack/AddCCFullVariableTrack.vt.json"
        
        elif checkpoint == 'CONSOLIDATEDFUNDING':
            checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/BREFullVariableTrack/BREFullVariableTrack.vt.json?token=TfJMtn7nTXEDuwnnzB64ViLmIHya_SPZHcrQegk_0lKNLBcZ"
            file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/BREFullVariableTrack/BREFullVariableTrack.vt.json"

        elif checkpoint == 'WITHDRAWALATTEMPT':
            checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/WithdrawalFullVariableTrack/WithdrawalFullVariableTrack.vt.json?token=TfJMtn7nTXEDuwnnzB64ViLmIHya_SPZHcrQegk_0lKNLBcZ"
            file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/WithdrawalFullVariableTrack/WithdrawalFullVariableTrack.vt.json"

        else:
            pass
        var_checkpoint= "  match(c:Checkpoint{{name:'{checkpoint}'}}) \
                        merge (v:Var{{name:'{varname}'}}) \
                        set v.update_dt = '{update_dt}' \
                        create unique (c)-[co:CHECKPOINT_OF]->(v) return co"
        var_set = set()
        """
        html = urllib2.urlopen(checkpoint_url, context=self.CONTEXT)
        json_file = json.loads(html.read())
        """
        with open(file_path, "r") as read_file:
            json_file = json.load(read_file)
        for varclass in json_file['classBasedVariables']:
            var = varclass.split('.')[0]
            var_set.add(var)

        for varname in json_file['configurableVariables']:
            var_set.add(varname)
        
        for varname in var_set:
            print varname
            #print var_checkpoint.format(checkpoint=checkpoint, varname=varname, update_dt=self.UPDATE_DT)
            graph.cypher.execute(var_checkpoint.format(checkpoint=checkpoint, varname=varname, update_dt=self.UPDATE_DT))

    def get_eve_keylib_from_code(self):
        """
            weekly fetch from unified-parent keylib, and set var.is_key = True

        """
        key_url = "https://github.paypal.com/raw/DART/unified-parent/SH/unified-variables/src/main/java/com/paypal/risk/idi/library/keylib/KeyLib.java?token=AAAR5bN6SP0qebX2coDa8jLPMueuvmrNks5dQRwLwA"
        html = urllib2.urlopen(key_url, context=self.CONTEXT)
        
        soup = BeautifulSoup(html, 'html.parser')
        var_iskey = " merge (v:Var{{name:'{varname}'}}) \
                        set v.is_key = True"
        for line in soup:
            if 'AbstractBaseVariable' in line:
                keyset = re.findall("(?<=AbstractBaseVariable)(.*)(?==)", line)
                if len(keyset) >0:                     
                    for evekey in keyset:
                        varname = evekey.replace(' ','')
                        graph.cypher.execute(var_iskey.format(varname=varname))

    def parse_variable_metadata(self):
        """
            weekly parse variable_metadata.json from compiling targets from variable-metadata code
        """
        with open("/Users/metang/workspace/variable-metadata/major-variable-metadata/target/classes/metadata/variable/variable_metadata.json", "r") as read_file:
            data = json.load(read_file)
            for item in data:
                try:
                    variable_type = item['type']
                    variable_name = item['name']
            
                    if  variable_type == 'RADD':
                        radd_name = item['table_name']
                        field_name = item['field_name']
                        radd_key = item['keys']
                        var_entity = self._create_var_entity(variable_type, variable_name)
                        utils.radd_handler(field_name, radd_name , radd_key, var_entity)

                    elif variable_type == 'WRITING_EDGE':
                        container_name = item['container_type']              
                        aggregation_type = item['aggregation_type']  # e.g. cnt
                        value_type = item['value_type'] #e.g. sliding window
                        var_entity = self._create_raw_edge_entity( variable_name, aggregation_type, value_type)

                        utils.edge_handler(container_name,'','',var_entity)


                    elif variable_type == 'READING_EDGE' and item['edge_type'] == 'Decay':
                        container_key = item['container_key']
                        container_name = item['container_type']
                        edge_type = item['edge_type'] # e.g. decay
                        var_entity = self._create_reading_edge_entity(container_key, variable_name, edge_type)

                        utils.edge_handler(container_name, container_key,'' , var_entity)


                    elif variable_type == 'READING_EDGE'and item['edge_type'] <> 'Decay':
                        container_name = item['container_type']
                        container_key = item['container_key']
                        raw_edge = item['corresponding_variable']
                        edge_type = item['edge_type'] # e.g. decay
                        var_entity = self._create_reading_edge_entity(container_key, variable_name, edge_type)

                        utils.edge_handler(container_name,container_key, raw_edge , var_entity)
                except:
                    pass
        #decay edge is depending on itself:
        graph.cypher.execute("match(v:Var) where v.name contains '_dk_' and v.edge_type='Decay'  create unique (v)-[:DEPEND_ON]-(v)")


    def _create_var_entity(self, variable_type, variable_name):
        """Creates the variable entity and the remote node in the Euler datastore.

        Args:
            var: a dictionary with the variable data.
        Returns:
            The newly created entity.
        """
        try:
            print 'need update ', var['variable_name']
            var_entity = Var(name=var['variable_name'],
                            type=var['variable_type'])
            var_entity.create_node()
            return var_entity
        except ValueError:
            pass

    def _create_raw_edge_entity(self, variable_name, aggregation_type, edge_type):

        var_entity = Var(name=variable_name ,
                         type='EDGE',
                         aggregation_type = aggregation_type,
                         edge_type = edge_type,
                         is_raw_edge = True)
        var_entity.create_node()
        return var_entity

    def _create_reading_edge_entity(self, container_key, variable_name, edge_type):

        var_entity = Var(name=variable_name,
                         type='EDGE',
                         container_key= container_key,
                         edge_type = edge_type,
                        )
        var_entity.create_node()
        return var_entity


GitUpdator().parse_variable_metadata()
#GitUpdator().get_checkpoint_var_from_code('WITHDRAWALATTEMPT')