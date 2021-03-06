from utils import utils
from entities.watermark import Watermark
from entities.var import Var
from entities.radd import RADD
from entities.model import Model
from entities.edge_container import EdgeContainer
import ssl
import json
import urllib2
import time
from utils.graph_db import graph
from utils import mappings

VARIABLE_CATALOG_BASE_URL = 'http://10.176.1.109:3000/api/variables'  # NOQA
VARIABLE_CATALOG_CP_URL ='http://10.176.1.109:3000/api/enum'
VARIABLE_CATALOG_TOKEN = 'a1f98939c5b8cea62b099e5fb13cf5b7'
RUCS_DEP_URL = 'https://ccg22riskunifiedmgmt4152.ccg22.lvs.paypalinc.com/api/component/rucs/dependency?variable={variable}'
CONTEXT = ssl._create_unverified_context()



class VarCatalog(object):
    def __init__(self, service):

        self.variable_types = set(['Component', 'Context', 'EDGE', 'LION', 'Cache'
                                  'OTF', 'RADD', 'Trinity', 'Tier1'])
        self.radds = dict()
        self.models = dict()
        self.edge_containers = dict()
        self.watermark = Watermark('VarCatalog_'+service)
        self.watermark.create_node()
        self.max_datetime = self.watermark.last_updated
        self.variable_catalog_full_url = VARIABLE_CATALOG_BASE_URL + '/?token=' + VARIABLE_CATALOG_TOKEN + '&select=all&service=' + service
        self.variable_catalog_type_url = VARIABLE_CATALOG_BASE_URL + '/?token=' + VARIABLE_CATALOG_TOKEN + \
                                    '&select=variable_status,variable_name,check_points,variable_type&service=rtcs&check_points={checkpoint}'
        self.variable_catalog_cp_url = VARIABLE_CATALOG_CP_URL + '/?token=' + VARIABLE_CATALOG_TOKEN + '&checkpoints'

    def run_raw_edge(self):
        html = urllib2.urlopen(self.variable_catalog_full_url, context=CONTEXT)
        time.sleep(30)
        self.variables = json.loads(html.read())
        self._load_raw_edge()
        self.watermark.update_properties()



    def run_rtcs_var_all(self):
        """Main class function.

        Loads new variables to the datastore.
        Updates the Watermark entity with the newest release datetime.
        """
        html = urllib2.urlopen(self.variable_catalog_full_url, context=CONTEXT)
        time.sleep(30)
        self.variables = json.loads(html.read())
        self._load_variable()
        self.watermark.update_properties()

    
    def _load_variables(self):
        """Iterates over all Variables from the Variable Catalog and loads to db.

        Only processes relevent varialbe types and new variables.
        """
        for var in self.variables:
            try:
                if len(var['variable_status']) > 1 and var['variable_status'] == 'Retired':
                    self._retire_variable_from_db(var['variable_name'])

                elif self._should_process_variable(var):
                    var_entity = self._create_var_entity(var)
                    self._create_non_variable_entities(var, var_entity)              
                    self._update_max_datetime(utils.date_str_to_datetime(var['release_date']))
            except:
                pass
        # force to set type = 'OTF' (instead of 'EDGE') if it was a variable based on derived edge, e.g. rcvr_diff_auth_amt_dk_160:
        graph.cypher.execute("match(otf:Var)<-[:DEPEND_ON]-(derived:Var)-[:DEPEND_ON]->(raw:Var) where raw.is_raw_edge is null and derived.type='EDGE'  set derived.type='OTF'")

        self.watermark.last_updated = self.max_datetime

    def _should_process_variable(self, var):
        """Checks if var should be processed.

        Args:
            var: a dictionary with the variable data.
        Returns:
            True if the variable should be processed, otherwise False.
        """
        if 'variable_type' in var and 'release_date' in var and len(var['release_date']) > 0:
            if var['variable_type'] in self.variable_types:
                var_release_datetime = utils.date_str_to_datetime(var['release_date'])
                return var_release_datetime > self.watermark.last_updated


    def _create_var_entity(self, var):
        """Creates the variable entity and the remote node in the datastore.

        Args:
            var: a dictionary with the variable data.
        Returns:
            The newly created entity.
        """
        try:
            print 'need update ', var['variable_name']
            var_entity = Var(name=var['variable_name'],
                            release_date=var['release_date'],
                            status=var['variable_status'],
                            type=var['variable_type'])
            var_entity.create_node()
            return var_entity
        except ValueError:
            pass


    def _create_raw_edge_entity(self, var):

        print 'need update ', var['variable_name']
        var_entity = Var(name=var['variable_name'],
                         release_date=var['release_date'],
                         status=var['variable_status'],
                         code_link=var.get('code_link', ''),
                         type='EDGE',
                         is_raw_edge = True)
        var_entity.create_node()
        return var_entity

    def _create_non_variable_entities(self, var, var_entity):
        """Handles the variable dependencies (RADD, Model, Edge Container).

        Args:
            var: the variable object (from the Variable Catalog).
            var_entity: Var object corresponding to the var object.
        """

        try:
            if 'table_name' in var and var['table_name']  <> 'N/A':
                utils.radd_handler(var['field_name'], var['table_name'] , '', var_entity)
            if 'used_models' in var:
                self._model_handler(var, var_entity)
            if 'edge_container' in var:
                utils.reading_edge_handler(var['edge_container'],var['edge_key'], var['edge_value_key'], var_entity)

        except AttributeError:

            pass

    def _update_var_type(self, var, var_entity):
        try:
            if len(var['check_points'])  >1:
                for item in var['check_points']:
                    utils.checkpoint_handler()
                

        except AttributeError:

            pass

    def _model_handler(self, var, var_entity):
        """Handler method for Model dependencies.

        Creates the Model entities and relationship to the variable.

        Args:
            var: the variable object (from the Variable Catalog).
            var_entity: Var object corresponding to the var object.
        """
        self._create_model_entities(var['used_models'])
        for model_name in var['used_models']:
            model_entity = self.models[model_name]
            model_entity.create_unique_relationship('CONSUMES', var_entity.node)

    def _create_model_entities(self, models_list):
        """Creates Model entities and their corresponding nodes in the datastore.

        Args:
            models_list: a list of Model names.
        """
        for model_name in models_list:
            if model_name not in self.models:
                self.models[model_name] = Model(name=model_name)
                self.models[model_name].create_node()

    def _load_raw_edge(self):
        for var in self.variables:

            if len(var['variable_status']) > 1 and var['variable_status'] == 'Retired':
                self._retire_variable_from_db(var['variable_name'])

            elif self._should_process_variable(var):
                var_entity = self._create_raw_edge_entity(var)
                self._create_non_variable_entities(var, var_entity)

                self._update_max_datetime(utils.date_str_to_datetime(var['release_date']))
        self.watermark.last_updated = self.max_datetime



        
    def _update_max_datetime(self, release_datetime):
        """Updates self.max_datetime if release_datetime is newer.

        Args:
            release_datetime: datetime object.
        """
        if release_datetime > self.max_datetime:
            self.max_datetime = release_datetime

    def _retire_variable_from_db(self, variable):
        statement = (
           " MATCH(v:Var {{name: '{variable}'}}) detach delete v"
        )
        graph.cypher.execute(statement.format(variable=variable))

    def _get_rucs_dependency(self, variable):
 
        html = urllib2.urlopen(RUCS_DEP_URL.format(variable = variable))
        json_file = json.loads(html.read())
        container_statement = (
            "MATCH (e:EdgeContainer {{name: '{edge_container}'}}) MATCH(v:Var {{name: '{variable}'}}) create unique (v) -[rel:ATTRIBUTE_OF]->(e) return rel"
        )
        var_statement = (
            "MATCH (v1:Var {{name: '{variable}'}}) MATCH(v2:Var {{name: '{upstream_variable}'}}) create unique (v1) -[rel:{relation}]->(v2) return rel "
        )
        radd_statement = (
            "MATCH (r:Radd {{name: '{radd}'}}) MATCH(v:Var {{name: '{variable}'}}) create unique (v) -[rel:QUERIES]->(e) return rel"
        )
        
        if json_file['computeVertexs']:
            upstream = json_file['computeVertexs']
            type_list = []
            for item in upstream:
                type_list.append(item['type'])
            if 'DATA_LOAD' in type_list:
                relation='USING_KEY'
            else:
                relation = 'DEPEND_ON'

            for item in upstream:
               
                if item['type'] == 'DATA_LOAD':
                    data_type = item['name'].split('.')[:3]
                    if data_type[1] == 'EDGE_CONTAINER':
                        edge_container = data_type[2]
                        statement = container_statement.format(edge_container=edge_container, variable=variable)
                    elif data_type[1] == 'RADD':
                        radd = data_type[2]
                        statement = radd_statement.format(radd=radd, variable=variable)
                elif item['type'] == 'VARIABLE':
                    upstream_variable = item['varName']
                    statement = var_statement.format(variable=variable, upstream_variable=upstream_variable,relation=relation)
                else:
                    statement = False
                if statement:
                    try:
                        graph.cypher.execute(statement)
                    except Exception as e:
                        print e
                        pass

    def get_checkpoint_var(self):
        html = urllib2.urlopen(self.variable_catalog_cp_url, context=CONTEXT)
        checkpoints = json.loads(html.read())

        for item in checkpoints:
            if item not in ['BRE', 'POS', 'EDGE']:
                #will remove this pieve of code when onboardinggc is merged into one; then we will use case insensitive mapping
                if item in mappings.VC_CP_MAPPING.keys():
                    item = mappings.VC_CP_MAPPING[item]
                graph.cypher.execute("MERGE(c:Checkpoint{{name: '{item}'}}) return c.name".format(item = item))
                self.run_rtcs_var_type(item)
                

    def run_rtcs_var_type(self, checkpoint):
        html = urllib2.urlopen(self.variable_catalog_type_url.format(checkpoint=checkpoint), context=CONTEXT)
        if checkpoint in mappings.VC_CP_MAPPING.keys():
            checkpoint = mappings.VC_CP_MAPPING[checkpoint]            
        time.sleep(5)
        variables = json.loads(html.read())
        existing_var_set = set()
        new_var_set = set()
        ret = graph.cypher.execute("match(v:Var)-[]-(c:Checkpoint{{name:'{checkpoint}'}}) return v.name as name order by name".format(checkpoint=checkpoint))
        for variable in ret:
            existing_var_set.add(variable.name)
        for var in variables:
            if var['variable_status'] <> 'Retired':
                new_var_set.add(var['variable_name'])

        utils.update_var_checkpoint(checkpoint, new_var_set-existing_var_set, existing_var_set-new_var_set)  

    def _load_var_type_checkpoint(self):
        for var in self.variables:
            try:
                if len(var['variable_status']) > 1 and var['variable_status'] == 'Retired':
                    self._retire_variable_from_db(var['variable_name'])

                elif self._should_process_variable(var):
                    var_entity = self._create_var_entity(var)
                    self._create_non_variable_entities(var, var_entity)              
                    self._update_max_datetime(utils.date_str_to_datetime(var['release_date']))
            except Exception as e:
                print e
                pass
        # force to set type = 'OTF' (instead of 'EDGE') if it was a variable based on derived edge, e.g. rcvr_diff_auth_amt_dk_160:
        graph.cypher.execute("match(otf:Var)<-[:DEPEND_ON]-(derived:Var)-[:DEPEND_ON]->(raw:Var) where raw.is_raw_edge is null and derived.type='EDGE'  set derived.type='OTF'")

        self.watermark.last_updated = self.max_datetime

    def run_test(self):
        query = "match(v:Var) return v.name as name"
        ret = graph.cypher.execute(query)
        for var in ret:
            varname = var['name'].rstrip()

            self._get_rucs_dependency(varname)


VarCatalog('rtcs').get_checkpoint_var()