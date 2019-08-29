from local_entity import LocalEntity


class LocalVar(LocalEntity):
    """A "Local Var" entity (from Pluto rule).

    Inherits from LocalEntity.

    Attributes:
        properties: a dictionary of the entity's properties
    """

    def __init__(self, **properties):
        """Calls LocalEntity constructor with the given properties dict"""
        super(LocalVar, self).__init__('LocalVar', **properties)
