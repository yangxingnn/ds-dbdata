#!/usr/bin/python
# coding:utf-8

'''delete outdated data in the database by postgreSQL
@author : yangccnn
@version : 1.0
@todo : created in 2017-11-15
'''

import os
import ConfigParser
import psycopg2
import sys

class DsData(object):

    def __init__(self,env_name,conf_rela_path):
        self.env_home = os.getenv(env_name)
        if(self.env_home == None):
            sys.exit(1)
        
        self.conf_rela_path = conf_rela_path
        self.conf_path = self.env_home + self.conf_rela_path
        self.conn = None
    
    def read_config(self,post_section='postgreSQL',ds_section='db_backup'):
        conf = ConfigParser.ConfigParser()
        try:
            conf.read(self.conf_path)
        except IOError as e:
            print e
            sys.exit(1)
        self.username = conf.get(post_section,'username')
        self.password = conf.get(post_section,'password')
        self.dbname = conf.get(post_section,'dbname')
        self.host = conf.get(post_section,'host')
        self.port = conf.get(post_section,'port')
        self.backup_path = conf.get(ds_section,'backup_path')
        self.schemas = conf.get(ds_section,'schemas').split(',')
        self.except_tables = conf.get(ds_section,'except_tables').split(',')
        self.interval = int(conf.get(ds_section,'interval'))
        


    def get_conn(self):
        try:
            self.conn = psycopg2.connect(database=self.dbname,user=self.username,password=self.password,host=self.host,port=self.port) 
        except BaseException as e:
            print e
            sys.exit(1)
        return self.conn

        
    def close_conn(self):
        if(self.conn):
            self.conn.close()


    def sava_data(self):
        self.conn.set_client_encoding('utf-8')
        cur = self.conn.cursor()
        self.get_table_dict(cur,self.schemas)
        
        # gzip 

    def get_table_dict(self,cur,schemas):
        for schema in schemas:
            cur.execute('SELECT tablename FROM pg_tables WHERE schemaname = \'%s\'' %schema)
            rows = cur.fetchall()
            print rows
            #  if not rows:
                #  print rows

    #  def delete_outdata(self):
        

if __name__ == '__main__':
    ds_data = DsData('DS_DATADB_HOME','/config/config')
    ds_data.read_config()
    ds_data.get_conn()
    ds_data.sava_data()
    #  ds_data.delete_outdata()


