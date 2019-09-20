from utils.graph_db import graph
from py2neo import Relationship


class BaseEntity(object):
    """Base entity.

    Implements base interactions with the chosen data store.
    This should be used as the base class for any entity in the application.

    Attributes:
        node: data store object representing the entity (py2neo.Node)
        properties: a dictionary of the entity's properties
    """

    def __init__(self, **properties):
        """Inits self.node to None and self.properties to properties"""
        self.node = None
        self.properties = properties

    def create_relationship(self, rel_type, end_node, **props):
        """Creates a relationship from this node to 'end_node'
        Args:
            rel_type: the relatinoship type
            end_node: py2neo.Node object
            props (optional): one or more relationship properties
        """
        new_relationship = Relationship(self.node, rel_type, end_node, **props)
        graph.create(new_relationship)

    def create_unique_relationship(self, rel_type, end_node, **props):
        """Creates a unique relationship from this node to 'end_node'
        Args:
            rel_type: the relatinoship type
            end_node: py2neo.Node object
            props (optional): one or more relationship properties
        """
        statement = (
            'MATCH (start_node), (end_node)\n'
            'WHERE id(start_node) = {start_id} AND id(end_node) = {end_id}\n'
            'MERGE (start_node)-[rel:{rel_type}]->(end_node)\n'
            'RETURN rel'
        ).format(start_id=self.node._id, end_id=end_node._id, rel_type=rel_type)
        rel = graph.cypher.execute(statement).one
        rel.properties.update(**props)
        rel.push()

    def update_properties(self, **properties):
        """Updates this object's properties and self.node properties
        Args:
            properties: new properties to update
        """
        statement = 'MATCH (n) WHERE id(n) = {node_id} SET n={props} RETURN n'
        # merge current properties with new properties
        self.properties.update(properties)
        params = {'node_id': self.node._id, 'props': self.properties}
        self.node = graph.cypher.execute(statement, params).one
        self.node.properties.pull()

    def match_relationships(self, rel_type=None, end_node=None,
                            bidirectional=False, limit=None):
        """Matches all relationships from this entity's node to end_node
        Args:
            rel_type (optional): type of relationship (any type if None)
            end_node (optional): bound end node to match (any node if None)
            bidirectional (optional): True if incoming relationships should be included
            limit (optional): maximum number of relationship to match (no limit if None)
        Returns:
            matching relationships (generator)
        """
        return graph.match(self.node, rel_type, end_node, bidirectional, limit)
