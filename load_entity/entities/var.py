from global_entity import GlobalEntity
import re


class Var(GlobalEntity):
    """An "Variable" entity.

    Inherits from GlobalEntity.

    Attributes:
        properties: a dictionary of the entity's properties
    """

    def __init__(self, **properties):
        """Normalizes the Variable name.

        Calls GlobalEntity constructor with:
            - unique attribute name: name
            - label: Var
            - properties
        """
        properties['name'] = properties['name']
        super(Var, self).__init__(uniq_attr_name='name', label='Var', **properties)
        self.node = self.create_node()



