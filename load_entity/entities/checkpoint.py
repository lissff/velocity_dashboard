from global_entity import GlobalEntity
from utils.graph_db import graph


class Checkpoint(GlobalEntity):

    def __init__(self,**properties):
        properties['name'] = properties['name'].upper()
        super(Checkpoint, self).__init__(uniq_attr_name='name', label='Checkpoint', **properties)
        self.node = self.create_node()

    def create_relationship(self, node):
        self.create_unique_relationship('CHECKPOINT_OF',node)

    def single_delete_relationship(self, node,):
        statement = (
            'MATCH (start_node)-[rel:CHECKPOINT_OF]->(end_node)\n'
            'WHERE id(start_node) = {start_id} AND id(end_node) ={end_id}\n'
            'DELETE rel'
        ).format(start_id=self.node._id, end_id=node._id)
        graph.cypher.execute(statement)

    def batch_add_rule_relationship(self, rule_id_list):
        statement = (
            'MATCH (start_node),(end_node:Rule)\n'
            'WHERE id(start_node) = {start_id} AND end_node.rule_id in {rule_list}\n'
            'MERGE (start_node)-[rel:CHECKPOINT_OF]->(end_node)\n'
            'RETURN rel'
        ).format(start_id=self.node._id, rule_list=str(rule_id_list))
        graph.cypher.execute(statement)

    def batch_delete_rule_relationship(self, rule_id_list):
        statement = (
            'MATCH (start_node)-[rel:CHECKPOINT_OF]->(end_node:Rule)\n'
            'WHERE id(start_node) = {start_id} AND end_node.rule_id in {rule_list}\n'
            'DELETE rel'
        ).format(start_id=self.node._id, rule_list=str(rule_id_list))
        graph.cypher.execute(statement)
