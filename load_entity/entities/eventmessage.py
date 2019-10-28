from global_entity import GlobalEntity
import re


class EventMessage(GlobalEntity):

    def __init__(self, **properties):
        super(EventMessage, self).__init__(uniq_attr_name='name', label='EventMessage',
                                            **properties)