import json
import urllib2
from difflib import get_close_matches
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import ssl
from datetime import date
from graph_db import graph
from bs4 import BeautifulSoup
import re



def vc_api():

    url = 'http://10.176.1.98:3000/api/variables/?token=a1f98939c5b8cea62b099e5fb13cf5b7&variable_name_exact={variable}&select=variable_name,edge_key,edge_value_key&service=rtcs'

    with open("derived_edge.txt", "r") as read_file:
        with open('vc_edge_raw.txt', 'w') as out:
            for variable in read_file:
                html = urllib2.urlopen(url.format(variable = variable.rstrip()))
                json_file = json.loads(html.read())
                try:
                    if json_file[0]['edge_value_key']:
                        print variable.rstrip()+'|'+json_file[0]['edge_value_key']
                        out.write(variable.rstrip()+'|'+json_file[0]['edge_value_key']+'\n')
                except:
                    continue


def vc_raw_edge():
    url = 'http://10.176.1.98:3000/api/variables/?token=a1f98939c5b8cea62b099e5fb13cf5b7&service=edge'
    #with open("derived_edge.txt", "r") as read_file:
    html = urllib2.urlopen(url.format(variable = variable.rstrip()))
    json_file = json.loads(html.read())
    with open('raw_edge_from_vc.txt', 'w') as out:
        for item in json_file:
            variable = item['variable_name']
            status = item['variable_status']

            if status <> 'Retired':
                print variable.rstrip()+'|'+json_file[0]['edge_value_key']
                out.write(variable.rstrip()+'|'+json_file[0]['edge_value_key']+'\n')


def naming_similarity():
    """
    acct_login_len63_s_cnt_dist_num_ip_1m
    acct_pymt_atmpt_sent_cnt_90_1h
    acct_rcvr_cnt_ratio_sndr_1d
    vid_addbank_63_num_acct_1h
    s_ms_attempt_cnt_15
    s_r_ms_attempt_cnt_15
    BALSNT_pct_dk_2
    dyson_sender_acct_comp_version
    ip_ratio_max_dk_5
    vid_login_63_num_acct_1h
    """
    #key_eventfilter_func_target_slots_factor_timewindow
    key = ['acct','appguid','bcncookie','Bowserhash','bnum','bssid','cchash','dy','dyson','vid',\
            'email','etag','flname','fso','ip','latlong2','naddress','mid','r','s','supCookie','s_r']

    with open("derived_edge.txt", "r") as derived:
        with open('similarity_edge.txt', 'w') as out:
            for derived_item in derived:
                #print derived_item.rstrip()
                choices = []
                with open('raw_edge.txt', 'r') as raw:
                    for raw_item in raw:

                        if raw_item.rstrip().startswith(derived_item[:7]):

                            choices.append(raw_item.rstrip())
                            #print raw_item, derived_item
                    #print choices.__len__()
                    if choices.__len__() > 0:

                        removed_window = '_'.join(derived_item.split('_')[:-1])
                        print removed_window
                        result = process.extractOne(removed_window, choices)
                        raw = result[0]
                        ratio = str(result[1])
                        out.write(derived_item.rstrip()+'|'+raw+'|'+ratio+'\n')




def rucs_dependency():
    """
    match(v:Var) where NOT (v)-[:DEPEND_ON]-(:Var) and v.type = 'EDGE' and v.is_raw_edge is null return v.name
    """
    url = 'http://10.176.4.64:8080/v1/risk/management-portal/component/rucs/dependency?variable={variable}'

    with open("derived_edge.txt", "r") as read_file:
        with open('rucs_edge.txt', 'w') as out:
            for variable in read_file:
                html = urllib2.urlopen(url.format(variable = variable))
                json_file = json.loads(html.read())
                if json_file['computeVertexs']:
                    upstream = json_file['computeVertexs']
                    for item in upstream:
                        if item['type'] == 'VARIABLE':
                            upstream_var = item['varName']
                            print variable, upstream_var
                            out.write(variable.rstrip()+'|'+upstream_var+'\n')

def parse_eve_metadata():
    with open("metadata.json", "r") as read_file:
        data = json.load(read_file)
        with open('metadata_eve.txt', 'w') as out:

            for item in data:
                variable_type = item['type']
                variable_name = item['name']


                if  variable_type == 'RADD':
                    radd_name = item['table_name']
                    field_name = item['field_name']
                    radd_key = item['keys']
                    #out.write(variable_name+'|'+)
                elif variable_type == 'READING_EDGE' and item['edge_type'] == 'Decay':
                    out.write(variable_name+'\n')

                elif variable_type == 'READING_EDGE'and item['edge_type'] <> 'Decay':
                    container_name = item['container_type']
                    #container_key = item['container_key']
                    raw_edge = item['corresponding_variable']
                    edge_type = item['edge_type']
                    #out.write(variable_name+'|'+container_name+'|'+raw_edge+'\n')

    """
                else:
                    container_name = item['container_type']
                    #container_key = item['container_key']
                    raw_edge = ''
                    edge_type = item['edge_type']
                    #out.write(variable_name+'|'+container_name+'|'+raw_edge+'\n')
    """

def get_key_via_rucs():
    """
    match(v:Var) where v.container_key is null and v.type='EDGE' return v.name
    """
    url = 'http://10.176.4.64:8080/v1/risk/management-portal/component/rucs/dependency?variable={variable}'

    with open("edge_no_key.txt", "r") as read_file:
        with open('rucs_edge_no_key.txt', 'w') as out:
            for variable in read_file:
                html = urllib2.urlopen(url.format(variable = variable))
                json_file = json.loads(html.read())
                if json_file['computeVertexs']:
                    upstream = json_file['computeVertexs']
                    for item in upstream:
                        if item['type'] == 'VARIABLE':
                            upstream_var = item['varName']
                            print variable, upstream_var
                            out.write(variable.rstrip()+'|'+upstream_var+'\n')


def remove_retired():
    url = 'http://10.176.1.98:3000/api/variables/?token=a1f98939c5b8cea62b099e5fb13cf5b7&service=edge'
    #with open("derived_edge.txt", "r") as read_file:
    html = urllib2.urlopen(url)
    json_file = json.loads(html.read())

    reired = set()
    edge_live = set()
    rtcs_live = set()
    with open('iras_retired.txt', 'r') as out:
        for item in out:
            reired.add(item.rstrip())
    print len(reired)



    for item in json_file:
        variable = item['variable_name']
        status = item['variable_status']

        if status <> 'Retired':
            edge_live.add(variable)
    print len(edge_live)
    #print edge_live

    url = 'http://10.176.1.98:3000/api/variables/?token=a1f98939c5b8cea62b099e5fb13cf5b7&service=rtcs'
    #with open("derived_edge.txt", "r") as read_file:
    html = urllib2.urlopen(url)
    json_file = json.loads(html.read())

    for item in json_file:
        variable = item['variable_name']
        status = item['variable_status']

        if status <> 'Retired':
            rtcs_live.add(variable)
    print len(rtcs_live)
    print len(reired - rtcs_live)
    print len(reired - rtcs_live - edge_live)

    with open('final_retired.txt', 'w') as out:
        for item in (reired - rtcs_live - edge_live):
            out.write(item+'\n')



def get_key_via_vc():
    url = 'http://10.176.1.98:3000/api/variables/?token=a1f98939c5b8cea62b099e5fb13cf5b7&select=variable_name,edge_key,edge_value_key&service=rtcs'

    html = urllib2.urlopen(url)
    json_file = json.loads(html.read())
    with open("edge_no_key.txt", "r") as read_file:
        with open('vc_edge_no_key.txt', 'w') as out:
            try:
                for variable in read_file:
                    for item in json_file:
                        if variable.rstrip() == item['variable_name'] and  len(item['edge_key']) >1:

                            print (variable.rstrip()+'|'+item['edge_key']+'\n')
                            out.write(variable.rstrip()+'|'+item['edge_key']+'\n')
            except:
                pass



def get_radd_key_via_rucs():

    url = 'http://10.176.4.64:8080/v1/risk/management-portal/component/rucs/dependency?variable={variable}'

    with open("radd_var.txt", "r") as read_file:
        with open('radd_var_key_rucs.txt', 'w') as out:
            for variable in read_file:
                print variable

                html = urllib2.urlopen(url.format(variable = variable))
                json_file = json.loads(html.read())
                if json_file['computeVertexs']:
                    upstream = json_file['computeVertexs']
                    for item in upstream:
                        if item['type'] == 'VARIABLE':
                            upstream_var = item['varName']
                            print variable, upstream_var
                            out.write(variable.rstrip()+'|'+upstream_var+'\n')

def get_full_dependency():
    pass



                    
                    
                    
parse_eve_key()


