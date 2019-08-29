from py2neo import Node
from utils.graph_db import graph
from base_entity import BaseEntity


class LocalEntity(BaseEntity):
    """Local Euler Entity

    Implements local entity specific interactions with the chosen data store.
    This should be used as the base class for any local entity in the Euler application.

    Attributes:
        label: the entity type (LocalVar, AggValue, ...)
        properties: a dictionary of the entity's properties
    """

    def __init__(self, label, **properties):
        """Inits self.label.

        Calls BaseEntity constructor with the given properties.
        """
        super(LocalEntity, self).__init__(**properties)
        self.label = label

    def create_node(self, new=False):
        """Creates the entity node in the datastore
        Args:
            new: a boolean to toggle between self.label and self.label + 'New'
                 Default value - False (self.label)
        Returns:
            The entity node that was created
        """
        label = self.label + 'New' if new else self.label
        self.node = Node(label, **self.properties)
        graph.create(self.node)
        return self.node
