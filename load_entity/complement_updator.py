"""
find edge/radd/otf node that is not connected to key or writing edge;
trying to connect them again by searching in code/rucs/vc
"""

def get_lonely_node():
    EDGE_NO_KEY='MATCH (v:Var) where not(v)-[:USING_KEY]->(:Var) and v.type='EDGE' and v.is_raw_edge is null  return  v.name as name'
    RADD_NO_KEY='MATCH (v1:Var) where v1.type='RADD' and not(v1)-[]-(:Var) return v1.name as name'

    for var in ret:
        varname = var['name'].rstrip()

