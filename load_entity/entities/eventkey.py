from global_entity import GlobalEntity
import re


class EventKey(GlobalEntity):

    def __init__(self, **properties):
        super(EventKey, self).__init__(uniq_attr_name='name', label='EventKey',
                                            **properties)

