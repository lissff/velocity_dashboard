from global_entity import GlobalEntity


class RuleVar(GlobalEntity):
    """A "Rule Variable" entity.

    Inherits from GlobalEntity.

    Attributes:
        properties: a dictionary of the entity's properties
    """

    def __init__(self, **properties):
        """Calls GlobalEntity constructor with:
            - unique attribute name: name
            - label: RuleVar
            - properties
        """
        super(RuleVar, self).__init__(uniq_attr_name='name', label='RuleVar',
                                      **properties)
