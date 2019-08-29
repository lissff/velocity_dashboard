from global_entity import GlobalEntity
import re


class RADD(GlobalEntity):
    """A "Radd" entity.

    Inherits from GlobalEntity.

    Attributes:
        properties: a dictionary of the entity's properties
        used_fields: a list of the used RADD fields
    """

    def __init__(self, **properties):
        """Normalizes the Radd name.

        Calls GlobalEntity constructor with:
            - unique attribute name: name
            - label: Radd
            - properties

        Inits used_fields to an empty list.
        """
        properties['name'] = self.name_normalizer(properties['name'])
        super(RADD, self).__init__(uniq_attr_name='name', label='Radd', **properties)
        self.used_fields = 'n/a'

    def name_normalizer(self, raw_radd_name):
        """Normalizes the RADD's name.

        Removes _RADD postfix and converts to all upper case.

        Args:
            raw_radd_name: the RADD name before normalization

        Returns:
            The normalized name (string).
        """
        normalized_name = re.sub(r'\s+|(_+RADD)*$', '', raw_radd_name, flags=re.IGNORECASE)
        return normalized_name.upper()

    def append_radd_field(self, radd_field):
        if self.used_fields == 'n/a':
            self.used_fields = [radd_field]
        else:
            self.used_fields.append(radd_field)

    def pluto_handler(self, pluto_entity):
        """Creates the relevant entities in the data store.

        Creates the following in the data store:
            - RADD node
            - (Pluto Entity)-[:QUERIES]->(Radd) relationship

        Args:
            pluto_entity: The Pluto entity object (Rule, Rule Variable, Local Variable).
                Needs to be bound to a remote node.
        """
        self.create_node()
        pluto_entity.create_unique_relationship('QUERIES', self.node,
                                                radd_fields=self.used_fields)