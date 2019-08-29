from global_entity import GlobalEntity
import re


class EdgeContainer(GlobalEntity):
    """An "Edge Container" entity.

    Inherits from GlobalEntity.

    Attributes:
        properties: a dictionary of the entity's properties
        used_variable: the edge variable used by this edge container

    Inits used_variable to 'n/a'
    """

    def __init__(self, **properties):
        """Normalizes the Edge Container name.

        Calls GlobalEntity constructor with:
            - unique attribute name: name
            - label: EdgeContainer
            - properties

        Inits used_variable to 'n/a'.
        """
        properties['name'] = self.name_normalizer(properties['name'])
        super(EdgeContainer, self).__init__(uniq_attr_name='name', label='EdgeContainer',
                                            **properties)
        self.used_variable = 'n/a'

    def name_normalizer(self, raw_container_name):
        """Normalizes the Edge Container's name.

        Removes _CONTAINER postfix.
        Replaces the container name with an alias, if needed.
        Converts to all upper case.

        Args:
            raw_container_name: the container name before normalization

        Returns:
            The normalized name (string).
        """
        normalized_name = re.sub(r'_CONTAINER$', '', raw_container_name)
        special_names_mapping = {
            'ACCOUNT_SLIDING_WINDOW': 'ACCOUNT_SW',
            'ACCOUNT_3RD': 'ACCT_3RD',
            'DEVICE_APPLICATION': 'DEVICE_APP'
        }
        if normalized_name in special_names_mapping:
            normalized_name = special_names_mapping[normalized_name]
        return normalized_name.upper()

    def pluto_handler(self, pluto_entity):
        """Creates the relevant entities in the data store.

        Creates the following in the data store:
            - Edge Container node
            - (Pluto Entity)-[:CONSUMES]->(Edge Container) relationship

        Args:
            pluto_entity: The Pluto entity object (Rule, Rule Variable, Local Variable).
                Needs to be bound to a remote node.
        """
        self.create_node()
        pluto_entity.create_unique_relationship('CONSUMES', self.node,
                                                edge_variable=self.used_variable)
