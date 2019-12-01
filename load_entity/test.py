import syslog
import json_tools
import json
from jsondiff import diff

dataa = {}
datab = {}

seta = set()
setb = set()

with open("cp_variable_metadata.json", "r") as read_file:
    dataa = json.load(read_file)
    for item in dataa:
      
        seta.add((item))

with open("variable_metadata.json", "r") as read_file:
    datab = json.load(read_file)
    for item in datab:
        setb.add(tuple(item))


#print tuple((d.items())) for d in dataa


print seta 

