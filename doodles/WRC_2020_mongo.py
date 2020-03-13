# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # MondoDB
#
# Experiments loading records from WRC API directly into MongoDB.

from WRCUtils2020 import _isnull, _notnull, _checkattr, _jsInt, listify

MONGO_HOST = 'localhost'
MONGO_PORT = 32770

# #%pip install pymongo
import json
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


class MongoDB:
    """
    Simple MongoDB utils.
    
    Makes use of a single (default named) db although other dbs can be used.
    """
    
    def __init__(self, host=None, port=None, dbname=None):
        """Initialise class for handling MongoDB connection, databases and collections."""
        # A connection object to a MongoDB server
        self.mongo_conn = None
        # Connection objects to specified MongoDB databases
        self.mongo_dbs = {}
        # Connection objects to specified MongoDB collections
        self.mongo_colls = {}
        # The dbname is used for the "current working db"
        self.mongo_settings = {'host': host, 'port': port,
                               'dbname': dbname}

    def mongodb_connect(self, host=None, port=None):
        """Attempt to create db connection."""
        if host:
            self.mongo_settings['host'] = host
        elif not host and not self.mongo_settings['host']:
            self.mongo_settings['host'] = 'localhost'
        if port:
            self.mongo_settings['port'] = port
        elif not port and not self.mongo_settings['port']:
            self.mongo_settings['port'] = 27017

        try:
            mc = MongoClient(host=self.mongo_settings['host'],
                             port=self.mongo_settings['port'],
                             serverSelectionTimeoutMS=1000)
            mc.server_info()
            self.mongo_conn = mc
        except ServerSelectionTimeoutError:
            self.mongo_conn = None

    def mongodb_set_dbname(self, dbname='wrc_timing'):
        """Set the default database name."""
        self.mongo_settings['dbname'] = dbname
        
    def mongodb_checkconn(self, host=None, port=None):
        """Check existence of a MongoDB connection and try to create it otherwise."""
        if not _checkattr(self, 'mongo_conn'):
            self.mongodb_connect(host, port)

    def mongodb_checkdb(self, dbname=None, host=None, port=None):
        """Attempt to connect to a MongoDB database, creating it if required."""
        # TO DO: if really paranoid, we chould check the mongodb connection is live
        self.mongo_settings['dbname'] = dbname = dbname or self.mongo_settings['dbname']
        if not dbname:
            # Set a default dbname
            self.mongodb_set_dbname()
            dbname = self.mongo_settings['dbname']
        if dbname not in self.mongo_dbs:
            self.mongodb_checkconn(host, port)
            mc = self.mongo_conn
            if mc:
                self.mongo_dbs[dbname] = mc[dbname]
            else:
                self.mongo_dbs[dbname] = None

    def mongodb_checkcoll(self, collection=None, dbname=None):
        """Check a collection exists and create it otherwise."""
        if not collection or (collection in self.mongo_colls and self.mongo_colls[collection] is not None):
            return

        self.mongodb_checkdb(dbname)
        db = self.mongo_dbs[self.mongo_settings['dbname']]
        if db:
            self.mongo_colls[collection] = db[collection]
        else:
            self.mongo_colls[collection] = None

    def mongodb_upsert(self, collection=None,
                       query=None, data=None,
                       dbname=None):
        """Upsert a single record to a specified collection."""
        self.mongodb_checkcoll(collection, dbname=dbname)
        coll = self.mongo_colls[collection]
        if not coll:
            return

        if not query or not isinstance(query, dict):
            query = {}
        if data and isinstance(data, dict):
            if not query:
                coll.insert_one(data)
            else:
                coll.updateOne(query, {"$set": data})
                
    def mongodb_find_one(self, collection, query=None, dbname=None):
        """Simple mongodb query."""
        self.mongodb_checkcoll(collection, dbname=dbname)
        coll = self.mongo_colls[collection]
        if not coll:
            return
        
        query = query or {}
        if query:
            return coll.find_one(query)
        return coll.find_one()
    
    def mongofy(self, collection=None, query=None, data=None, dbname=None):
        """If we have a database, use it."""
        if hasattr(self, 'mongo_conn') and self.mongo_conn is not None:
            self.mongodb_upsert(collection=collection,
                                query=query, data=data,
                                dbname=dbname)
    
    def mongodb_list_dbs(self):
        """List of dbs on MongoDB connectio."""
        self.mongodb_checkconn()
        conn = self.mongo_conn
        if conn:
            return conn.list_database_names()

    def mongodb_list_colls(self, dbname=None):
        """List of dbs on MongoDB connectio."""
        self.mongodb_checkdb(dbname)
        db = self.mongo_dbs[self.mongo_settings['dbname']]
        if db:
            return db.list_collection_names()
    

# + tags=["active-ipynb"]
# mtest = MongoDB(MONGO_HOST, MONGO_PORT)
# mtest.mongodb_list_colls()

# + tags=["active-ipynb"]
# mtest.mongo_settings['dbname']

# + tags=["active-ipynb"]
# mtest.mongodb_upsert('season', data=json.loads(getActiveSeasonEvents(raw=True)))
# mtest.mongodb_list_colls()

# + tags=["active-ipynb"]
# mtest.mongo_colls

# + tags=["active-ipynb"]
# #mtest.mongodb_find_one('season')
