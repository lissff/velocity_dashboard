from utils import utils
from entities.watermark import Watermark
from entities.var import Var
from entities.radd import RADD
from entities.model import Model
from entities.edge_container import EdgeContainer
import ssl
import json
import urllib2
from utils.graph_db import graph

VARIABLE_CATALOG_BASE_URL = 'http://10.176.1.109:3000/api/variables'  # NOQA
VARIABLE_CATALOG_TOKEN = 'a1f98939c5b8cea62b099e5fb13cf5b7'
RUCS_DEP_URL = 'http://10.176.4.64:8080/v1/risk/management-portal/component/rucs/dependency?variable={variable}'
# VARIABLE_CATALOG_FULL_URL = VARIABLE_CATALOG_BASE_URL + '/?token=' + VARIABLE_CATALOG_TOKEN + '&select=all&service=rtcs'  # NOQA
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
        #self.add_rules = VarRuleUpdator().singleRun
        variable_catalog_full_url = VARIABLE_CATALOG_BASE_URL + '/?token=' + VARIABLE_CATALOG_TOKEN + '&select=all&service=' + service
        html = urllib2.urlopen(variable_catalog_full_url, context=CONTEXT)
        self.variables = json.loads(html.read())


    
    def run_raw_edge(self):
        self._load_raw_edge_to_euler()
        self.watermark.update_properties()

    def run(self):
        """Main class function.

        Loads new variables to the Euler datastore.
        Updates the Watermark entity with the newest release datetime.
        """
        self._load_variables_to_euler()
        self.watermark.update_properties()

    def _load_variables_to_euler(self):
        """Iterates over all Variables from the Variable Catalog and loads to Euler.

        Only processes relevent varialbe types and new variables.
        """
        for var in self.variables:
            try:
                if len(var['variable_status']) > 1 and var['variable_status'] == 'Retired':
                    self._retire_variable_from_db(var['variable_name'])

                #elif var['release_date'] == '' or var['variable_type'] == 'EDGE':
                #    var_entity = self._create_var_entity(var)
                #    self._create_non_variable_entities(var, var_entity)
                    #self.add_rules(var['variable_name'])
                elif self._should_process_variable(var):
                    var_entity = self._create_var_entity(var)
                    self._create_non_variable_entities(var, var_entity)
                    #self.add_rules(var['variable_name'])
                    self._update_max_datetime(utils.date_str_to_datetime(var['release_date']))
            except:
                pass
        self.watermark.last_updated = self.max_datetime

    def _should_process_variable(self, var):
        """Checks if var should be processed.

        Args:
            var: a dictionary with the variable data.
        Returns:
            True if the variable should be processed, otherwise False.
        """
        if 'variable_type' in var and 'release_date' in var :
            if var['variable_type'] in self.variable_types:
                var_release_datetime = utils.date_str_to_datetime(var['release_date'])
                return var_release_datetime > self.watermark.last_updated


    def _create_var_entity(self, var):
        """Creates the variable entity and the remote node in the Euler datastore.

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
            print var['variable_name'], var['release_date'],var['variable_status'],var['variable_type']
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
        #TODO:call rucs for more info
        try:
            if 'table_name' in var:
                self._radd_handler(var, var_entity)
            if 'used_models' in var:
                self._model_handler(var, var_entity)
            if 'edge_container' in var:
                self._edge_handler(var, var_entity)
            self._get_rucs_dependency(var['variable_name'])
        except AttributeError:

            pass
    def _radd_handler(self, var, var_entity):
        """Handler method for RADD dependencies.

        Creates the RADD entities and relationship to the variable.

        Args:
            var: the variable object (from the Variable Catalog).
            var_entity: Var object corresponding to the var object.
        """
        self._create_radd_entities(var['table_name'])
        if var['field_name'] is not None:
            radd_fields_lists = self._get_radd_fields(var['field_name'])
            if len(radd_fields_lists)>0:
                for radd_name, fields in zip(var['table_name'], radd_fields_lists):
                    radd_entity = self.radds[radd_name]
                    var_entity.create_unique_relationship('QUERIES', radd_entity.node,
                                                          radd_fields=fields)

    def _create_radd_entities(self, radds_list):
        """Creates RADD entities and their corresponding nodes in the Euler datastore.

        Args:
            radds_list: a list of Model names.
        """
        for radd_name in radds_list:
            if radd_name not in self.radds:
                self.radds[radd_name] = RADD(name=radd_name)
                self.radds[radd_name].create_node()

    def _get_radd_fields(self, fields):
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
        radd_fields_lists = [self._get_unique_fields(f) for f in radd_fields]
        return radd_fields_lists

    def _get_unique_fields(self, fields):
        """Remove fields duplicated

        Args:
            fields: A string of fields, seperated by tilde ('~').
        Returns:
            A list of unique fields (strings).
        """
        # remove None values from list and get unique values (set)

        unique_fileds = set([f for f in fields.split('~') if f is not None])
        return list(unique_fileds)

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
        """Creates Model entities and their corresponding nodes in the Euler datastore.

        Args:
            models_list: a list of Model names.
        """
        for model_name in models_list:
            if model_name not in self.models:
                self.models[model_name] = Model(name=model_name)
                self.models[model_name].create_node()

    def _load_raw_edge_to_euler(self):
        for var in self.variables:

            if len(var['variable_status']) > 1 and var['variable_status'] == 'Retired':
                self._retire_variable_from_db(var['variable_name'])
            elif var['release_date'] == '' :
                var_entity = self._create_raw_edge_entity(var)
                self._create_non_variable_entities(var, var_entity)

            elif self._should_process_variable(var):
                var_entity = self._create_raw_edge_entity(var)
                self._create_non_variable_entities(var, var_entity)

                self._update_max_datetime(utils.date_str_to_datetime(var['release_date']))
        self.watermark.last_updated = self.max_datetime


    def _edge_handler(self, var, var_entity):
        """Handler method for Edge Container dependencies.

        Creates the Edge Container entities and relationship to the variable.

        Args:
            var: the variable object (from the Variable Catalog).
            var_entity: Var object corresponding to the var object.
        """
        print 'need create raw edge' + var['edge_container']
        edge_name = var['edge_container']
        edge_key = var['edge_key']
        raw_edge = var['edge_value_key']
        if self._is_empty(edge_name):
            return      
        edge_entity = EdgeContainer(name=edge_name)
        edge_entity.create_node()
        var_entity.create_unique_relationship('ATTRIBUTE_OF', edge_entity.node)
        self.edge_containers[edge_name] = edge_entity

        if self._is_empty(edge_key):
            return
        key_entity = Var(name=edge_key)
        key_entity.create_node()
        var_entity.create_unique_relationship('USING_KEY', key_entity.node)

        if self._is_empty(raw_edge):
            return 
        raw_edge_entity = Var(name=raw_edge, is_raw_edge=True)
        raw_edge_entity.create_node()
        var_entity.create_unique_relationship('DEPEND_ON', raw_edge_entity.node)

    def _is_empty(self, str):
        if str is None or len(str) < 1:
            return True
        else:
            return False
        
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
                    except:
                        pass

    def run_test(self):
        query = "match(v:Var) where v.type='RADD' return v.name as name"
        ret = graph.cypher.execute(query)
        for var in ret:
            varname = var['name'].rstrip()

            self._get_rucs_dependency(varname)
#VarCatalog('edge').run_raw_edge()
#VarCatalog('rtcs').run()
VarCatalog('test').run_test()