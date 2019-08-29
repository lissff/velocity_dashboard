from global_entity import GlobalEntity


class Status(GlobalEntity):
    """Status entity, which indicates 'Success' or 'Failed' status.

    Inherits from GlobalEntity.
    """

    def __init__(self, status_type):
        """Calls GlobalEntity constructor with:
            - unique attribute name: type
            - label: Status
            - properties: {'type': status_type}
        """
        super(Status, self).__init__(uniq_attr_name='type', label='Status',
                                     type=status_type)
