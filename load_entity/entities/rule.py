from global_entity import GlobalEntity
from datetime import datetime
import re


class Rule(GlobalEntity):
    """A "Rule" entity.

    Inherits from GlobalEntity.

    Attributes:
        rule_id: An integer describing th rule ID
        name: The rule name
        priority: An integer describing the priority of the rule (lower is earlier)
        status: The current status (Active, Working, Disabled, ...)
        creator: The corp. name of the creator
        modifier: The corp. name of the modifier
        ownership: The team owning the rule (Seller, Consumer, Regional, ...)
        checkpoints: A list of the checkpoints where this rule is running
        creation_ts: A string representation of the creation timestamp
        modified_ts: A string representation of the modification timestamp
        path: The location of the rule in the rule engine
        rule_text: A string of the entire rule text
    """

    def __init__(self, rule_xml):
        """Calls GlobalEntity constructor with:
            - unique attribute name: rule_id
            - label: Rule
            - properties (without the rule text)

        Inits all of the rule's attributes using the rule_xml.
        """
        self.rule_id = int(rule_xml['@ruleID'])
        self.name = rule_xml['@name']
        self.priority = int(rule_xml['@priority_ext'])
        self.status = rule_xml['@status']
        self.creator = rule_xml['@created']
        self.modifier = rule_xml['@mod']
        self.ownership = rule_xml['@ownership']
        self.checkpoints = self._get_checkpoints_list(rule_xml['@Checkpoints'])
        self.creation_ts = self._normalize_rule_timstamp(rule_xml['@cre_date'])
        self.modified_ts = self._normalize_rule_timstamp(rule_xml['@mod_date'])
        self.path = rule_xml['@path']
        self.rule_text = self._get_rule_text(rule_xml.get('logic', ''),
                                             rule_xml.get('actions', {}))
        properties = vars(self).copy()
        properties.pop('rule_text')
        super(Rule, self).__init__(uniq_attr_name='rule_id', label='Rule', **properties)

    def _get_checkpoints_list(self, checkpoints_str):
        """Converts rule xml's checkpoints string to a list of checkpoints

        Args:
            checkpoints_str: a string in the format -
            "[checkpoint_1, checkpoint2, ...]"

        Returns:
            A list of checkpoints
        """
        clean_str = checkpoints_str.replace('[', '').replace(']', '')
        checkpoints_list = clean_str.split(', ')
        return checkpoints_list

    def _normalize_rule_timstamp(self, datetime_str):
        """Converts rule xml's timestamp attribute to normalized timestamp.

        Args:
            datetime_str: timestamp string in the following format -
            %a %b %d %H:%M:%S GMT+07:00 %Y OR
            %a %b %d %H:%M:%S MST %Y
            Example: Thu Feb 11 01:13:18 GMT+07:00 2016

        Returns:
            A datetime string in the following format -
            %Y-%m-%d %H:%M:%S
        """
        datetime_str = datetime_str.replace('GMT+07:00 ', '').replace('MST ', '')
        try:
            dt_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            dt_obj = datetime.strptime(datetime_str, '%a %b %d %H:%M:%S %Y')
        return str(dt_obj)

    def _get_rule_text(self, rule_logic, rule_actions):
        """Converts rule xml's logic and actions to one rule text string.

        Args:
            rule_logic: string
            rule_actions: a list of strings

        Returns:
            A string (rule's text)
        """
        if 'action' in rule_actions and isinstance(rule_actions['action'], list):
            actions = '\n'.join(a for a in rule_actions['action'] if a)
        else:
            actions = ''
        rule_text = u"{0}\n{1}".format(rule_logic, actions)
        rule_text = rule_text.replace(u'\u201c', '"').replace(u'\u201d', '"')
        return rule_text

    def get_rule_text_splitted(self):
        """Splits rule's text into tokens

        Seperate apostrophes and inverted commas from variable names etc.

        Returns:
            A list with all of the rule's tokens
        """
        return re.sub(r'(?<!\\)(["\'\[\];()])', r' \1 ', self.rule_text).split()
