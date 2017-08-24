# encoding: utf-8
from cassandra.cluster import Cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra import util
from cassandra.cqlengine import connection
import datetime

from numpy.random import randint

cluster = Cluster(['127.0.0.1', '127.0.0.2', '127.0.0.3'])
session = cluster.connect()

session.set_keyspace('test')

## Connection Setting
c = connection.setup(['127.0.0.1'], 'cqlengine', protocol_version=3)

## Create DB object
class test_cf(Model):
    __keyspace__='test'

    # user_id = columns.UUID(primary_key=True)
    # name = columns.Text()
    user = columns.Text(primary_key=True)
    prop1 = columns.Text()
    time = columns.Date()

## get limit
All = test_cf.all()

print("len(All.limit(10000))")
print(len(All.limit(10000)))

print("len(All.limit(None))")
print(len(All.limit(None)))

# Before -> Error missing 1 required
# After -> Result > 10000
print("len(All.limit())")
print(len(All.limit()))
