from utils.graph_db import graph
from base_entity import BaseEntity



class GlobalEntity(BaseEntity):
    """Global Euler Entity

    Implements global entity specific interactions with the chosen data store.
    This should be used as the base class for any global entity in the Euler application.

    Attributes:
        uniq_attr_name: name of the entity's unique attribute
        label: the entity type (Radd, EdgeContainer, Model, ...)
        properties: a dictionary of the entity's properties
    """

    def __init__(self, uniq_attr_name, label, **properties):
        """Inits self.uniq_attr_name and self.label.

        Calls BaseEntity constructor with the given properties.

        Raises:
            KeyError: In case uniq_attr_name is not found in the properties dict
        """
        self.uniq_attr_name = uniq_attr_name
        if uniq_attr_name not in properties:
            err_msg = 'missing attribute: {} in properties'.format(uniq_attr_name)
            raise KeyError(err_msg)
        super(GlobalEntity, self).__init__(**properties)
        self.label = label

    def create_node(self, new=False):
        """Creates the entity node in the datastore
        If node exists, this object's properties will be merged with the existing node.

        Args:
            new: a boolean to toggle between self.label and self.label + 'New'
                 Default value - False (self.label)

        Returns:
            The entity node that was created
        """
        label = self.label + 'New' if new else self.label
        statement = (
            'MERGE (e:{label} {{{uniq_attr_name}: {{uniq_attr_value}}}})\n'
            'RETURN e'
        ).format(label=label, uniq_attr_name=self.uniq_attr_name)
        params = {'uniq_attr_value': self.properties[self.uniq_attr_name]}
        print params
        self.node = graph.cypher.execute(statement, params).one
        self.update_properties(**self.node.properties)
        return self.node
