from gitcode_updator import GitUpdator;
from vc_updator import VarCatalog;
from eve_updator import EVEUpdator;

GitUpdator().incremental_parse_variable_metadata() # only parse updated information in variable_metadata.json
VarCatalog('rtcs').get_checkpoint_var() # get component exposure and variable type from vc
DOTParser().get_entities_id()  # add rucs information