load checkpoint mapping table:


LOAD CSV WITH HEADERS FROM  "file:////cp.csv" AS row
FIELDTERMINATOR ','
WITH row
Match (o: Ontology{name:row.Ontology}) Merge(c: ContextPath{name:row.Path}) merge (o)-[m:mapping]->(c) return m

LOAD CSV WITH HEADERS FROM  "file:////cp.csv" AS row
FIELDTERMINATOR ','
WITH row
Match (c: ContextPath{name:row.Path}) set c.auditing= row.Auditing

LOAD CSV WITH HEADERS FROM  "file:////cp.csv" AS row
FIELDTERMINATOR ','
WITH row
Match (c: ContextPath{name:row.Path}) set c.availability= row.Availability

LOAD CSV WITH HEADERS FROM  "file:////cp.csv" AS row
FIELDTERMINATOR ','
WITH row
Match (c: ContextPath{name:row.Path}) set c.sample= row.Sample


match(o:Ontology{name:'IP'}) match(ek:EventKey) match(em:EventMessage{name:'CreditCardCreated'}) \
where (em)-[:HAVE_KEY]->(ek)<-[:mapping]-(o) return ek


match(o:Ontology{name:'IP'}) match(ek:EventKey) match(em:EventMessage{name:'CreditCardCreated'}) \
where (em)-[:HAVE_KEY]->(ek)<-[:mapping]-(o) with ek match (ek)<-[:ATTRIBUTE_OF]-(v_raw:Var) \
where v_raw.type = 'EDGE' with v_raw match(v_raw)-[:DEPEND_ON]-(v_derived:Var) \
return v_derived.name as Name, v_derived.eve_key as Key, v_derived.target as Target, \
v_derived.function as Function,v_derived.type as Type ,v_derived.filter as Filter order by v_derived.name desc