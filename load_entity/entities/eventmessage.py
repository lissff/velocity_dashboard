from global_entity import GlobalEntity
import re


class EventMessage(GlobalEntity):

    def __init__(self, **properties):

        properties['name'] = self.name_normalizer(properties['name'])
        super(EventMessage, self).__init__(uniq_attr_name='name', label='EventMessage',
                                            **properties)

