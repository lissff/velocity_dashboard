import pymongo
from bson.objectid import ObjectId

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["edge-builder"]
rollup_keys = mydb["rollupkeys"]
messages = mydb["messages"]

keys_query = {"status":"production"}

keys = rollup_keys.find(keys_query)

for x in keys:

    #print(x['dtoReference'], x['name'], x['messageId'])
    messageId = x['messageId']
    message = messages.find_one({'_id': ObjectId(messageId)})
    if  message:
        print(message['name'], x['dtoReference'] )