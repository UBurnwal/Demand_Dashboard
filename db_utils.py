#!/usr/bin/env python
# coding: utf-8

# In[2]:


import yaml
import datetime
from sqlalchemy import create_engine, event
from pandasql import sqldf
import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import os
import time
import sys
import boto3
import requests
from pandas.io.sql import SQLTable


def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))


SQLTable._execute_insert = _execute_insert


def read_sql_file(filename, **kwargs):
    path = '{}'.format(filename)
    fd = open(path, 'r')
    sqlFile = fd.read()
    fd.close()
    extracted_kwargs = kwargs['arguments']
    return sqlFile.format(**extracted_kwargs)


def fetch_data(query, connection):
    credentials = yaml.safe_load(
        open(
            os.path.join(
                os.getcwd(),
                'config.yaml')))
    db_credentials = credentials['databases'][connection]
    query_start_ts = time.time()
    no_result = True
    while no_result:
        try:
            conn = psycopg2.connect(**db_credentials)
            df = pd.read_sql_query(query, conn)
            no_result = False
        except psycopg2.Error as error:
            print('There was an error with the database operation: {}'.format(error))
            conn.close()
        except exception as e:
            conn.close()
        finally:
            conn.close()
    seconds_taken = time.time() - query_start_ts
    print('Query run time in seconds : {}'.format(seconds_taken))
    return df
    

def __get_sqlalchemy_engine(db):
    credentials = yaml.safe_load(open(os.path.join(os.getcwd(), 'config.yaml')))

    database=credentials['databases'][db]['database']
    username = credentials['databases'][db]['user']
    host = credentials['databases'][db]['host']
    password = credentials['databases'][db]['password']
    port = credentials['databases'][db]['port']

    engine_query = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}".format(
        username = username,
        password=password,
        host=host,
        port=port,
        database=database
    )
    engine = create_engine(engine_query, echo = False)

    return engine


def write_to_db(table_dict, db = 'redshift_db'):
    redshift_engine = __get_sqlalchemy_engine(db)
    for key in table_dict:
        dataframe = table_dict[key]
        table_name = key
        dataframe['created_at'] = (datetime.datetime.now())
        dataframe.to_sql(
            table_name, redshift_engine, if_exists = 'append',
            chunksize = 1000, index = False
        )


def __get_s3_client():
    credentials = yaml.safe_load(
        open(
            os.path.join(
                os.getcwd(),
                'config.yaml')))
    client = boto3.resource(
        's3',
        aws_access_key_id=credentials['s3']['ACCESS_KEY'],
        aws_secret_access_key=credentials['s3']['SECRET_KEY'],
    )
    return client

def send_post_request():
    response = 404
    while response != 200:
        url = '/order_delay_offence/suspend'
        data = {'somekey': 'somevalue'}
        response = requests.post(url, data = myobj)
    return "Finished"

