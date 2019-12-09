from utils.graph_db import graph

QueryList = {
# reading edge and radd variable key check
"READING_EDGE_NO_KEY":"MATCH (v:Var) where not(v)-[:USING_KEY]->(:Var) \
                    and v.type='EDGE' and v.is_raw_edge is null  return  v.name as name",

"RADD_NO_KEY":"MATCH (v1:Var) where v1.type='RADD' and not(v1)-[]-(:Var) return v1.name as name",


# ontology mapping integrity check
"EVENTKEY_NO_ONTOLOGY":'match(ek:EventKey) where not (ek)-[]-(:Ontology) return ek.name as name',
"VARKEY_NO_ONTOLOGY":'match(v:Var)<-[:USING_KEY]-(:Var) where  not (v)-[]-(:Ontology) return v.name as name',

# duplicated key/rel check
"READING_EDGE_MULTIKEY": "match(v:Var)-[:USING_KEY]->(key:Var) where v.type='EDGE' with v,length(collect(key.name)) as len  where len > 1 with v match (v)-[:USING_KEY]->(key:Var)  return distinct v.name as name",
"DUPLICATED_RELATIONS" :'match (v1)-[rel]->(v2) with length(collect(rel)) as len, v1, v2 where len >1 return v1.name as name',

# retirement candidate check
"REMOVE_RETIRED_RADD": 'match(r:Radd) where not(r)-[]-(:Var) return r.name as name',

# problematic variables check
"PROBLEMATIC_VARIEBLES": "match(v:Var) where v.name contains 'VariableGroup' return v.name as name",

# edge variable relationship check
"RAW_EDGE_NO_DERIVING" : "MATCH (v:Var) where not(v)<-[:DERIVE_FROM]-(:Var) and v.is_raw_edge = true return v.name as name",

"LONELY_READING_EDGE": "MATCH (v:Var) where v.is_raw_edge is null and v.type='EDGE' and not(v)-[:DERIVE_FROM]->(:Var) return v.name as name",

"RAW_EDGE_NO_EVENTKEY":" MATCH (v:Var) where not(v)-[:ATTRIBUTE_OF]->(:EventKey) and v.is_raw_edge = true return distinct v.name as name"
}
def sending_alert():
    alert_file = open('./resources/alert_email.txt','w')
    for k in QueryList.keys():
        cypher_query = QueryList[k]
        ret = graph.cypher.execute(cypher_query)
        alert_file.write("------------------------------------------------------------------------------------------------------------"+'\n')
        alert_file.write("-------------------------------------"+k+"  start   -------------------------------------"+'\n')
        alert_file.write("------------------------------------------------------------------------------------------------------------"+'\n')

        if len(ret) > 0:
            for item in ret:
                alert_file.write(item.name+'\n')
        alert_file.write("------------------------------------------------------------------------------------------------------------"+'\n')
        alert_file.write("------------------------------------------------------------------------------------------------------------"+'\n')
    alert_file.close()
sending_alert()