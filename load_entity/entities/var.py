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
        properties['name'] = self.name_normalizer(properties['name'])
        super(Var, self).__init__(uniq_attr_name='name', label='Var', **properties)

    def name_normalizer(self, raw_var_name):
        """Normalizes the Variable's name.

        Removes (@|ARS_) prefix if needed.
        Converts to all lower case.

        Args:
            raw_var_name: the variable name before normalization

        Returns:
            The normalized name (string).
        """
        normalized_name = re.sub(r'^(@|ARS_)', '', raw_var_name, flags=re.IGNORECASE)
        return normalized_name
