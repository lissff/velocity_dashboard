from global_entity import GlobalEntity
import re


class OfflineTable(GlobalEntity):


    def __init__(self, **properties):

        super(OfflineTable, self).__init__(uniq_attr_name='name', label='OfflineTable', **properties)
        self.used_fields = 'n/a'



  
