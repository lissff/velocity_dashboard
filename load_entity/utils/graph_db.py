from py2neo import Graph
import os

# get User IDs and passwords
NEO4J_UID = ''
NEO4J_PWD = ''

# get neo4j db config
NEO4J_HOST = '10.176.6.146'
NEO4J_PORT = '7474'

graph = Graph('http://{0}:{1}@{2}:{3}/db/data/'.format(NEO4J_UID, NEO4J_PWD,
                                                       NEO4J_HOST, NEO4J_PORT))

# unique constraints tuples
nodes = [
    ('Watermark', 'type'),
    ('Model', 'name'),
    ('Radd', 'name'),
    ('EdgeContainer', 'name'),
    ('Status', 'type'),
    ('Var', 'name'),
    ('EventMessage', 'name')
]

# set unique constraints in neo4j graph db
for label, property in nodes:
    try:
        graph.schema.create_uniqueness_constraint(label, property)
    except:
        continue

index_nodes = [
    ('Var', 'name')
]

for label, property in index_nodes:
    try:
        graph.schema.create_index(label, property)
    except:
        continue


