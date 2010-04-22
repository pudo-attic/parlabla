

# leech bundestagger 

from couchdb.client import Server
from couchdb.tools.dump import dump_db

DB_NAME = 'bundestagger_speeches'

server = Server("http://10.0.1.21:5984/")
db = server[DB_NAME]

print dir(db)

for doc in db: 
    print doc.id
