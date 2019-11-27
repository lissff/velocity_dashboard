from gitcode_updator import GitUpdator;
from vc_rucs_updator import VarCatalog;
from eve_updator import EVEUpdator;

GitUpdator().parse_variable_metadata()
#VarCatalog('edge').run_raw_edge()
VarCatalog('rtcs').run_rtcs_var()
EVEUpdator().run_eve_updator()