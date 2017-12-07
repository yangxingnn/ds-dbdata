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
import datetime

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
        backup_path = conf.get(ds_section,'backup_path')
        self.backup_path = (backup_path + '/') if (len(backup_path) > 1 and not backup_path.endswith('/')) else backup_path
        self.schemas = conf.get(ds_section,'schemas').split(',')
        self.except_tables = conf.get(ds_section,'except_tables').split(',')
        self.interval = int(conf.get(ds_section,'interval'))    # 单位为天
        
    def get_conn(self):
        try:
            self.conn = psycopg2.connect(database=self.dbname,user=self.username,password=self.password,host=self.host,port=self.port) 
        except BaseException as e:
            print e
            sys.exit(1)
        return self.conn

    def process_data(self):
        self.get_conn()
        self.conn.set_client_encoding('utf-8')
        cur = self.conn.cursor()
        schema_dict = self.get_table_dict(cur,self.schemas)
        dl = datetime.timedelta(days = self.interval)
        today = datetime.datetime.now()
        d30 = (today - dl).strftime('%Y-%m-%d %H:%M:%S')     # interval间隔天前
        for schema, vallist in schema_dict.items():     # val: (table_name, collect_time_name)
            for tablename, collectname in vallist:
                filepath = ''.join([self.backup_path, self.dbname, '/', schema, '/']) 
                filename = ''.join([tablename, '_', today.strftime('%y%m%d%H%M%S'), '.csv.gz'])
                iscreate = self.create_file(filepath, filename)
                if iscreate:
                    save_sql = 'COPY (SELECT * FROM {sch_name}.{tab_name} WHERE {time_name} < \'{end_time}\')  TO PROGRAM \'gzip > {filepath}{filename}\' WITH (FORMAT csv)'.format(sch_name = schema,tab_name = tablename, time_name = collectname, end_time = d30,filepath = filepath, filename = filename)
                    cur.execute(save_sql)
                    delete_sql = 'DELETE FROM {sch_name}.{tab_name} WHERE {time_name} < \'{end_time}\''.format(sch_name = schema, tab_name = tablename, time_name = collectname, end_time = d30)
                    cur.execute(delete_sql)
                    self.conn.commit()
                else:
                    print '',join([filepath, filename, ' can not be created! so the associated data has not been bakup']) 
        cur.close()
        self.conn.close()

    def create_file(self, path, filename):
        # path 尾部要求带/
        try:
            exist_path = os.path.exists(path)
            if not exist_path:
                os.makedirs(path)
            isfile = os.path.isfile(path + filename)
            if not isfile:
                os.mknod(path + filename)
                os.system('chmod 666 ' + path + filename)
            return True
        except e:
            return False

    def get_table_dict(self,cur,schemas):
        schema_dict = {}
        for schema in schemas:
            cur.execute('SELECT tablename FROM pg_tables WHERE schemaname = \'%s\'' %schema)
            rows = cur.fetchall()
            for table in [i[0] for i in rows if (schema +'.' + i[0]) not in self.except_tables]:
                sql = 'SELECT column_name FROM information_schema.columns WHERE table_schema = \'{0}\' AND table_name = \'{1}\''.format(schema,table)
                cur.execute(sql)
                columns = cur.fetchall()
                columns = list(map(lambda x: x[0],columns))
                if schema not in schema_dict:
                    schema_dict[schema] = []
                if 'collect_time' in columns:
                    schema_dict[schema].append((table,'collect_time'))
                elif 'check_time' in columns:
                    schema_dict[schema].append((table,'check_time'))
        return schema_dict


if __name__ == '__main__':
    ds_data = DsData('DS_DATADB_HOME','/config/config')
    ds_data.read_config()
    ds_data.process_data()

