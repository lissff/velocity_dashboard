from global_entity import GlobalEntity
from utils.graph_db import graph
from datetime import datetime
from utils import utils
import pytz


class Watermark(GlobalEntity):
    """Watermark entity, which keeps the last updated date-time.

    Inherits from GlobalEntity.

    Attributes:
        last_updated: datetime object for this Watermark last update.
    """

    def __init__(self, entity_type):
        """Calls GlobalEntity constructor with:
            - unique attribute name: type
            - label: Watermark
            - properties: {'type': entity_type}
        Inits last_updated to None.
        """
        super(Watermark, self).__init__(uniq_attr_name='type', label='Watermark',
                                        type=entity_type)
        self.last_updated = None

    def create_node(self):
        """Creates the watermark node in the datastore

        If node doesn't exist, the 'last_updated' property will be set to default:
            RADDAlert - current US/Pacific time
            Other - 1970-01-01 00:00:00

        Returns:
            The watermark node that was created
        """
        self.node = super(Watermark, self).create_node()
        # set default 'last_updated' property if not exists
        if 'last_updated' not in self.node:
            if self.properties['type'] == 'RADDAlert':
                pacific = pytz.timezone('US/Pacific')
                pacific_now = datetime.now(pacific).replace(microsecond=0, tzinfo=None)
                self.last_updated = pacific_now
            else:
                self.last_updated = datetime(1970, 1, 1)
            self.update_properties()
        else:
            self.last_updated = utils.timestamp_to_datetime(self.node['last_updated'])
        return self.node

    def update_properties(self, **properties):
        """Updates this object's properties and self.node properties

        If self.last_updated is not None, add to given properties.

        Args:
            properties: new properties to update
        """
        if self.last_updated:
            properties.update(last_updated=str(self.last_updated))
        super(Watermark, self).update_properties(**properties)
