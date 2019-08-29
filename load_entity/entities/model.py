from global_entity import GlobalEntity
from var import Var


class Model(GlobalEntity):
    """An "Model" entity.

    Inherits from GlobalEntity.

    Attributes:
        properties: a dictionary of the entity's properties
        used_variable: the model variable (score, segment, ...) used by this model entity
    """

    def __init__(self, **properties):
        """Inits used_variable to None.

        Calls GlobalEntity constructor with:
            - unique attribute name: name
            - label: Model
            - properties
        """
        properties['name'] = self.name_normalizer(properties['name'])
        super(Model, self).__init__(uniq_attr_name='name', label='Model', **properties)
        self.used_variable = None

    def name_normalizer(self, raw_model_name):
        """Normalizes the Model's name.

        Converts to all upper case.

        Args:
            raw_model_name: the Model name before normalization

        Returns:
            The normalized name (string).
        """
        return raw_model_name.upper()

    def create_used_var(self, var_name):
        """Creates Var instance.

        Sets the instance to this object's used_variable.

        Args:
            var_name: the variable name used

        Returns:
            A Var instance
        """
        self.used_variable = Var(name=var_name)
        return self.used_variable

    def pluto_handler(self, pluto_entity):
        """Creates the relevant entities in the data store.

        Creates the following in the data store:
            - Model node
            - Used Var node (if needed)
            - (Pluto Entity)-[:CONSUMES]->(Model) relationship OR
              (Pluto Entity)-[:CONSUMES]->(Var)->[:CONSUMES]->(Model)

        Args:
            pluto_entity: The Pluto entity object (Rule, Rule Variable, Local Variable).
                Needs to be bound to a remote node.
        """
        self.create_node()
        used_var_name = self.used_variable.properties['name']
        if used_var_name.startswith('model_'):
            pluto_entity.create_unique_relationship('CONSUMES', self.node,
                                                    variable=used_var_name)
        else:
            self.create_used_var_node_and_relationship()
            pluto_entity.create_unique_relationship('CONSUMES', self.used_variable.node)

    def create_used_var_node_and_relationship(self):
        """Creates Model Variable node and relationship to this Node (model).

        Returns:
            The created Variable node (bound to remote node).
        """
        var_node = self.used_variable.create_node()
        self.create_unique_relationship('CONSUMES', var_node)
        return var_node
