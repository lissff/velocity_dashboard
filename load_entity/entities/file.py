from global_entity import GlobalEntity
import re


class File(GlobalEntity):


    def __init__(self, **properties):

        super(File, self).__init__(uniq_attr_name='name', label='File', **properties)
        self.used_fields = 'n/a'



  
