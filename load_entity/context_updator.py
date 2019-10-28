import json
import urllib2
import pandas as pd
import os
from flatten_json import flatten
from utils.graph_db import graph
class ContextUpdator(object):
    def __init__(self):
        self.CHECKPOINT = ['AddCC' , 'ConsolidatedFunding' , 'WithdrawalAttempt']
        self.COMPUTE_UNITS_URL = 'https://ccg22riskunifiedmgmt4152.ccg22.lvs.paypalinc.com/api/component/rucs/variable-available'
        self.CONTEXT_SCHEMA_URL = 'https://ccg22riskunifiedmgmt4152.ccg22.lvs.paypalinc.com/api/onboard-checkpoint?checkpoint={checkpoint}&flow=@MERGED'
        
    def _get_context_path_dependency(self, ctx_path):
        
        html = urllib2.urlopen(self.COMPUTE_UNITS_URL.format(ctx_path=ctx_path))
        json_file = json.loads(html.read())

    
    def _get_schema_from_rucs(self, checkpoint):

        html = urllib2.urlopen(self.CONTEXT_SCHEMA_URL.format(checkpoint=checkpoint))
        print self.CONTEXT_SCHEMA_URL.format(checkpoint=checkpoint)
        json_file = json.loads(html.read())
        ctx_paths = json_file[0]['contexts']
        for ctx_path in ctx_paths:
            print ctx_path
            #graph.cypher.execute('merge(ctx:Var) return v.name as name order by name')

    def _get_scheme_from_eve(self, checkpoint):
        file_path = './resources/{checkpoint}.json'
        filename = os.path.join(os.path.dirname(__file__), file_path.format(checkpoint=checkpoint))
        UPDATE_CHECKPOINT_SCHEMA = (
        " match(c:Checkpoint{{name:'{checkpoint}'}}) merge(s:ContextPath{{name:'{schema}'}}) \
          create unique (c)-[h:HAVE_KEY]->(s) return h"
        )

        with open(filename, "r") as read_file:
            data = json.load(read_file)
            """
            msgs = pd.io.json.json_normalize(data)
            for item in  msgs.keys():
                print item
            """
            schema = flatten(data, '.')
            for line in schema:
                schema = line.replace('request.body.context.','').replace('.0','')
                graph.cypher.execute(UPDATE_CHECKPOINT_SCHEMA.format(checkpoint=checkpoint, schema=schema))
        

    def _map_key_var(self):
        unmatched = set()
        filename = os.path.join(os.path.dirname(__file__), './resources/key_var.txt')
        CHECK_PATH = (
        " match(s:ContextPath{{name:'{schema}'}}) \
          return s.name"
        )
        with open(filename, "r") as read_file:
            for line in read_file:
                if (graph.cypher.execute(CHECK_PATH.format(schema=line.rstrip())).one is None):
                    #print line
                    unmatched.add(line.rstrip())
        for item in unmatched:

            print item

#ContextUpdator()._map_key_var()
ContextUpdator()._get_scheme_from_eve('FundingPartnerPayments')