# encoding: utf-8
from cassandra.cluster import Cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra import util
from cassandra.cqlengine import connection
import cassandra
import datetime
from numpy.random import randint

from cassandra.cqlengine.management import create_keyspace_simple

KEYSPACE = 'test_keyspace'
TABLE = "test_table"
COLUMNS_NUM = 10100

cluster = Cluster(['127.0.0.1', '127.0.0.2', '127.0.0.3'])
session = cluster.connect()

## Connection Setting
c = connection.setup(['127.0.0.1'], 'cqlengine', protocol_version=3)

## Create DB object
class test_table(Model):
    __keyspace__=KEYSPACE

    user = columns.Integer(primary_key=True)
    prop = columns.Text()

def create_database():
    print("----- CREATE KEYSPACE & TABLE -----")

    create_keyspace_simple(KEYSPACE, replication_factor=3)
    session.set_keyspace(KEYSPACE)

    try:
        session.execute("CREATE TABLE " + TABLE + " (user int, prop text, PRIMARY KEY (user))")
    except cassandra.AlreadyExists:
        print('TABLE ' + TABLE + " is already exists")


def insert_data(num=1):
    print("----- INSERT COLUMNS -----")

    INSERT_USER = 0
    INSERT_PROP = ['AAAAA', 'BBBBB', 'CCCCC']

    while (INSERT_USER < num):
        test_table.create(user=str(INSERT_USER), prop=INSERT_PROP[randint(3)])
        INSERT_USER += 1


if __name__ == '__main__':

    create_database()
    insert_data(COLUMNS_NUM)

    session.set_keyspace(KEYSPACE)

    ## get limit
    All = test_table.all()

    print("len(All.limit(10000))")
    print(len(All.limit(10000)))
    print('')

    print("len(All.limit(None))")
    print(len(All.limit(None)))
    print('')

    # Before -> Error missing 1 required
    # After -> Result > 10000
    print("len(All.limit())")
    print(len(All.limit()))
