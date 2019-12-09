import syslog
import pygraphviz as pgv 
import networkx as nx

"""
LOAD CSV WITH HEADERS FROM  "file:////radd_dep.csv" AS row
FIELDTERMINATOR ','
WITH row, split(row.down,  '|') as down_vars
UNWIND down_vars AS each_val
MERGE (v: Radd{name: row.radd}) with v, each_val MATCH (v2: Var{name: each_val}) create unique (v2)-[:QUERIES]->(v) 

"""

"""
LOAD CSV WITH HEADERS FROM  "file:////radd_dep.csv" AS row
FIELDTERMINATOR ','
WITH row, split(row.key,  '|') as keys, split(row.down, '|') as vars
UNWIND vars as var
with var, keys  UNWIND keys AS key
MATCH (v: Var{name:var}) MATCH (v2: Var{name: key}) where not(v)-[:USING_KEY]->(v2)
and not v.name contains 'IDI_'
create unique (v)-[:USING_KEY]->(v2)

"""

"""
using variable dependency to calibrate issues caused by radd-var dependencies, e.g. multiple keys, variable groups
but need to be super careful about the deletions
"""
"""
LOAD CSV WITH HEADERS FROM  "file:////var_dep.csv" AS row
FIELDTERMINATOR ','
WITH row, split(row.key,  '|') as keyset
UNWIND keyset AS each_key
MATCH (v: Var{name: row.var})-[rel:USING_KEY]->(vk:Var) where v.type='RADD' 
and not vk.name in keyset delete rel
"""

"""
LOAD CSV WITH HEADERS FROM  "file:////var_dep.csv" AS row
FIELDTERMINATOR ','
WITH row, split(row.key,  '|') as keyset
UNWIND keyset AS each_key
MATCH (v: Var{name: row.var})-[]-(r:Radd) MATCH (v2: Var{name: each_key}) where v.type = 'RADD' and not (v)-[:USING_KEY]->(v2) and not (v)-[:DEPEND_ON]->(v2) merge(v)-[:USING_KEY]->(v2)
"""
class DOTParser():
    def __init__(self):
        self.graph = pgv.AGraph('./resources/dag.dot')
        radd_out_csv = './resources/radd_dep.csv'
        edge_out_csv = './resources/edge_dep.csv'
        variable_csv = './resources/var_dep.csv'
        self.radd_file = open(radd_out_csv, "w")
        self.edge_file = open(edge_out_csv, "w")
        self.variable_file = open(variable_csv, "w")
        
    def get_entities_id(self):
        
        for item in self.graph.iternodes():
            try:
                item_type = item.attr['label'].split('.')[0]
                if item_type in ['data', 'variable']: 

                    data_type = item.attr['label'].split('.')[1]
                    if item_type == 'data':
                        data_name = item.attr['label'].split('.')[2].split('|')[0]
                        if data_type =='RADD':
                            downstream, keyset = self.handle_radd(item)
                            if len(downstream) > 0:
                                self.radd_file.write (data_name+','+'|'.join(downstream)+','+'|'.join(keyset)+'\n')                          
                        elif data_type =='EDGE_CONTAINER':

                            self.handle_edge(item)

                    elif item_type == 'variable':
                        data_name = item.attr['label'].split('.')[1].split('|')[0]
                        downstream = self.handle_variable(item)
                        if len(downstream) > 0:
                            self.variable_file.write (data_name+','+'|'.join(downstream)+'\n')  

            except IndexError as e:
                print e, item
                pass


    def handle_variable(self, edge_item):

        down_list = []
      
        for item in self.graph.out_neighbors(edge_item):
            
            ret = self._parse_label(item)
            if ret and ret[0] == 'variable':
                down_list.append(ret[1])
      
        return down_list

    def handle_radd(self, radd_item):
        var_list = []
        key_list = []
        for item in self.graph.in_neighbors(radd_item):
            ret = self._parse_label(item)
            if ret and ret[0] == 'variable':
                var_list.append(ret[1])

        for item in self.graph.out_neighbors(radd_item):
            ret = self._parse_label(item)
            if ret and ret[0] == 'variable':
                key_list.append(ret[1])

        return var_list, key_list
           


    def handle_edge(self, edge_item):
        var_list = []
        key_list = []
        for item in self.graph.in_neighbors(edge_item):
            ret = self._parse_label(item)
            if ret and ret[0] == 'variable':
                var_list.append(ret[1])
        for item in self.graph.out_neighbors(edge_item):
            ret = self._parse_label(item)
            if ret and ret[0] == 'variable':
                key_list.append(ret[1])

        return var_list, key_list
        

    def _parse_label(self, label_string):
        if len(label_string.attr['label'].split('.')) > 1:
            item_type = label_string.attr['label'].split('.')[0]
            item_name = label_string.attr['label'].split('.')[1].split('|')[0]
            return item_type, item_name

    def get_entity_dependencies(self):
        pass


    def dot2csv(self):
        pass
        # variable_name|upstream_variable_list|edge_container_name|radd_table_name|reading_keys

    def __del__(self):
        self.radd_file.close()
        self.edge_file.close()
        self.variable_file.close()



DOTParser().get_entities_id()