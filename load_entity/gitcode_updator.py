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

        self.Event_Mapping = {'IAcctCreditCardEvent':'CreditCardCreated', \
                    'IAcctSellerDisputeEvent':'DisputeNotification',\
                    'IAcctSellerFaEvent':'UserFundsAvailabilityEvent',\
                    'IDisputeCaseSender':'DisputeLifecycle',\
                    'ITxnEventFunding':'Withdrawal',\
                    'ITxnSenderPaymentAttempt':'PaymentAttempt',
                    'ITxnEventSender':'PaymentDebit' }
        
    def run_checkpoint_mapping(self):
        ret = graph.cypher.execute('match(cp:Checkpoint) return cp.name as name order by name')

        for cp in ret:
            self.get_checkpoint_var_from_code(cp.name)

    def get_checkpoint_var_from_code(self, checkpoint):
        """
            weekly fetch checkpoint -[:CHECKPOINT_OF] - var relationship from unified-parent

        """
        if checkpoint == 'ConsolidatedFunding':
            #checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/BREFullVariableTrack/BREFullVariableTrack.vt.json?token=TfJMtn7nTXEDuwnnzB64ViLmIHya_SPZHcrQegk_0lKNLBcZ"
            file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/BREFullVariableTrack/BREFullVariableTrack.vt.json"

        elif checkpoint == 'WithdrawalAttempt':
            #checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/WithdrawalFullVariableTrack/WithdrawalFullVariableTrack.vt.json?token=TfJMtn7nTXEDuwnnzB64ViLmIHya_SPZHcrQegk_0lKNLBcZ"
            file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/WithdrawalFullVariableTrack/WithdrawalFullVariableTrack.vt.json"
         
        elif checkpoint == 'FundingPOS':
            file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/POSFullVariableTrack/POSFullVariableTrack.vt.json"
    
        else:
            #checkpoint_url="https://github.paypal.com/raw/DART/unified-parent/develop/unified-models/src/main/resources/updatable-config-models/WithdrawalFullVariableTrack/WithdrawalFullVariableTrack.vt.json?token=TfJMtn7nTXEDuwnnzB64ViLmIHya_SPZHcrQegk_0lKNLBcZ"
            file_path="/Users/metang/workspace/unified-parent/unified-models/src/main/resources/updatable-config-models/"+checkpoint+"FullVariableTrack/"+checkpoint+"FullVariableTrack.vt.json"
         

        var_checkpoint= "  match(c:Checkpoint{{name:'{checkpoint}'}}) \
                        merge (v:Var{{name:'{varname}'}}) \
                        set v.update_dt = '{update_dt}' \
                        create unique (c)-[co:CHECKPOINT_OF]->(v) return co"
        var_set = set()
        """
        html = urllib2.urlopen(checkpoint_url, context=self.CONTEXT)
        json_file = json.loads(html.read())
        """
        try:
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
            
        except:
            print 'no such file found' + checkpoint
            pass

    def get_eve_keylib_from_code(self):
        """
            weekly fetch from unified-parent keylib, and set var.is_key = True

        """
        file_path = "/Users/metang/workspace/unified-parent/unified-variables/src/main/java/com/paypal/risk/idi/library/keylib/KeyLib.java"
        with open(file_path, "r") as read_file:
            #json_file = json.load(read_file)

            soup = BeautifulSoup(read_file, 'html.parser')
        var_iskey = " merge (v:Var{{name:'{varname}'}}) \
                        set v.is_key = True"
        for line in soup:
            if 'AbstractBaseVariable' in line:
                keyset = re.findall("(?<=AbstractBaseVariable)(.*)(?==)", line)
                if len(keyset) >0:                     
                    for evekey in keyset:
                        print evekey
                        varname = evekey.replace(' ','')
                        graph.cypher.execute(var_iskey.format(varname=varname))

    def parse_variable_metadata(self):
        """
            weekly parse variable_metadata.json from compiling targets from variable-metadata code
        """
        with open("/Users/metang/workspace/variable-metadata/target/classes/metadata/variable/variable_metadata.json", "r") as read_file:
            data = json.load(read_file)
            rest_data = self._remove_existing_var(data)
            for item in rest_data:
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
                        value_type = item['value_type'] #e.g. sliding window
                        message_name = self._map_eventmessage(item['updated_logic'][0]['triggered_event'])
                        event_key = self._map_eventkey(item['updated_logic'][0]['key_expression'])

                        #flattern logical decay writing edge case in EVE
                        if item['edge_type'] == 'Decay':
                            for aggr_type in item['aggregation_type']:
                                for factor in item['factors']:
                                    variable_name = variable_name+'_'+lower(aggr_type)+'_dk_'+factor
                                    var_entity = self._create_raw_edge_entity(variable_name, aggr_type, value_type)
                                    utils.writing_edge_handler(container_name,var_entity, message_name, event_key )

                        else:
                            aggregation_type = item['aggregation_type']  # e.g. cnt
                            var_entity = self._create_raw_edge_entity( variable_name, aggregation_type, value_type)
                            print message_name,event_key
                            utils.writing_edge_handler(container_name,  var_entity, message_name, event_key )
                            


                    elif variable_type == 'READING_EDGE' and item['edge_type'] == 'Decay':
                        container_key = item['container_key']
                        container_name = item['container_type']
                        edge_type = item['edge_type'] # e.g. decay
                        var_entity = self._create_reading_edge_entity(container_key, variable_name, edge_type)

                        utils.reading_edge_handler(container_name, container_key, variable_name , var_entity)
                
                    elif variable_type == 'READING_EDGE' and item['edge_type'] == 'TSNC' and 'tsnc' not in variable_name:
                        container_key = item['container_key']
                        container_name = item['container_type']
                        edge_type = item['edge_type'] # e.g. decay
                        raw_edge = item['corresponding_variable']+'_last_upd_ts'
                        var_entity = self._create_reading_edge_entity(container_key, variable_name, edge_type)
                        utils.reading_edge_handler(container_name, container_key, raw_edge , var_entity)


                    elif variable_type == 'READING_EDGE':
                        container_name = item['container_type']
                        container_key = item['container_key']
                        raw_edge = item['corresponding_variable']
                        edge_type = item['edge_type'] # e.g. decay
                        var_entity = self._create_reading_edge_entity(container_key, variable_name, edge_type)

                        utils.reading_edge_handler(container_name, container_key, raw_edge , var_entity)
                    
                except Exception as e:
                    print e
                    pass

        #decay edge is depending on itself:
        

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

    def _remove_existing_var(self, dataset):
        varset = set()
        retset = []
        ret = graph.cypher.execute('match(v:Var) return v.name as name order by name')
        for variable in ret:
            varset.add(variable.name)
        for i in dataset:
            if not (i['name'] in varset):
                retset.append(i)
        return retset

        
    def _only_process_edge(self, dataset):
       
        retset = []
       
        for i in dataset:
            if (i['type'] == 'READING_EDGE' ):
                retset.append(i)
        return retset

    def _create_var_entity(self, variable_type, variable_name):
        """Creates the variable entity and the remote node in the datastore.

        Args:
            var: a dictionary with the variable data.
        Returns:
            The newly created entity.
        """
        try:
    
            var_entity = Var(name=variable_name,
                            type='RADD')
            var_entity.create_node()
            return var_entity
        except ValueError:
            pass
    def _map_eventmessage(self, message):


        if message in self.Event_Mapping.keys():
            event_message = self.Event_Mapping[message]
            return event_message

    def _map_eventkey(self, eventkey):
        message_name = eventkey.split('.')[1]
        if message_name in self.Event_Mapping.keys():
            event_key = eventkey.replace(message_name, self.Event_Mapping[message_name])
            return event_key



GitUpdator().parse_variable_metadata()
#GitUpdator().get_checkpoint_var_from_code('PartnerEvalFI')
#GitUpdator().get_eve_keylib_from_code()
#GitUpdator().run_checkpoint_mapping()