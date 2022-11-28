# %% 
import json
from typing import List, Union
import sqlalchemy
import pandas as pd
import numpy as np
import sys
import mysql.connector
from datetime import date, datetime
from package.logging_helper import logger
import urllib.parse
# with open(r"D:\PythonBySteven\config.json", "r") as f:
with open("config.json", "r") as f:
    config = json.load(f)

# %%
archive_table_mapping = pd.DataFrame({
  "archive_server" : ["vau", "vau2", "vau3", "vau4", 
                     "vuk", "vuk2", "vuk3", "vuk4", "vuk5", 
                     "vgp_uk", "mxt", "vcn", "vcn2" , 
                     "vt2", "vt3", 'mxt', 'mxt', 'pug', 'pug2', 
                     'bhs1', 'bhs2', 'bhs3',
                     "icn", "iuk", "iuk2", 
                     "iv", "ocn", "opl"],

  "mt4_server" : ["enfaureport", "enfau2report", "enfau3report", "enfau4report", 
                 "enfukreport", "enfuk2report", "enfuk3report", "enfuk4report", "enfuk5report", 
                 "vuk2report", "mxtreport", "nzgft_live1_vfx_report", "vcn2report", 
                 "vtm2report", "vtm3report", "vtm1report", "kcmreport",  'puireport', 'puireport', 
                 "bhs1report", "bhs2report", "bhs3report",
                 "infinoxcnreport", "gomarketreport", "infinox2report",
                 "ivreport", "originreport", "oplreport"]
})
# 由於歸檔庫vtm1report, kcmreport, mxtreport歸類在mxt歸檔table, 故刪除vtm1reporot, kcmreport.
archivedb_to_server = archive_table_mapping[~archive_table_mapping.mt4_server.isin(['vtm1report', 'kcmreport'])]

# %%
def get_report_engine(db, schema = None, db_type="mysql_db"):
    """Return a sqlalchemy connector."""

    user=config[db_type][db]["username"]
    password=config[db_type][db]["password"]
    host=config[db_type][db]["host"]
    port=config[db_type][db]["port"]
    # 針對會對Url連線造成影響(@)的特殊符號進行編碼
    password = urllib.parse.quote_plus(password)
    if db_type == "mysql_db":
        connector = f"mysql+pymysql://{user}:{password}@{host}:{port}"
    elif db_type == "maria_db":
        connector = f"mysql+pymysql://{user}:{password}@{host}:{port}"
    elif db_type == "mssql_db":
        connector = f"mssql+pyodbc://{user}:{password}@{host}:{port}"

    if schema is not None and db_type != 'mssql_db':
        connector = connector + (f"/{schema}?charset=utf8mb4")
    
    if schema is not None and db_type == "mssql_db":
        connector = connector + (f"/{schema}?driver=SQL+Server")
    # print(f'{connector}')
    return sqlalchemy.create_engine(connector)

# %%
def get_data(query:str, db:str, schema:Union[str, List], db_type:str = "mysql_db") -> pd.DataFrame:
    """
    傳送query從db拿取一或數個schema裡面table的資料，返回pandas.DataFrame
    Arguments: 
    query: Str, SQL字串
    db: Str(參照config), 要連結的Database
    schema: List/Str, 要使用的一或多個schema
    db_type: mysql_db/ mssql_db/ maria_db
    """
    if type(schema) is list:
        df = []
        for schema_name in schema:
            logger.info(f"{schema_name}")
            conn = get_report_engine(db, schema_name, db_type)
            temp = pd.read_sql(query, conn)
            conn.dispose()
            df.append(temp)
        df = pd.concat(df)
    elif type(schema) is str:
        logger.info(f"{schema}")
        conn = get_report_engine(db, schema, db_type)
        df = pd.read_sql(query, conn)
        conn.dispose()

    # 針對同步庫copy小寫名字對應
    if "server" in df.columns:
        conditions = [(df.server.eq('mt5_vfx_live')),
                    (df.server.eq('mt5_pug_live'))]
        results = ['MT5_VFX_Live', 'MT5_PUG_Live']
        df['server'] = np.select(conditions, results, default=df.server)
    return df

# %%
def write_table(df:pd.DataFrame, schema:str, table:str, col:List, db = 'datatp_manager', db_type = "mysql_db"):
    # conn = get_report_engine(db, schema, db_type)

    if len(df)<1:
        return 'empty data'
    
    df = df.where(pd.notnull(df), None)

    try:
        col = [ f'`{x}`' for x in col]
        conn = mysql.connector.connect(
            host=config[db_type][db]["host"],
            user=config[db_type][db]["username"],
            password=config[db_type][db]["password"],
            database=schema,
            auth_plugin='mysql_native_password'
        )
        cursor = conn.cursor(buffered=True)
        
        s = ['%s'] * len(col)  #  col數量
        query = f'''
            REPLACE INTO `{schema}`.`{table.replace('`','')}`
                ({', '.join(col)})
            VALUES
                ({', '.join(s)})
        '''
        if len(df)>1:
            values = df.values.tolist()
            for i in range(1+len(values)//10000):
                cursor.executemany(query, values[i*10000:(i+1)*10000])
                conn.commit()
        else:
            for val in df.values:
                val = tuple(val)
                cursor.execute(query, val)
            conn.commit()
            
        res = f'success : insert {schema}.{table}'
        logger.info(f'success : insert {schema}.{table}')
    except Exception as error:
        conn.rollback()
        res = f'failed database : insert {schema}.{table}'
        logger.exception(error)
    finally:
        cursor.close()
        conn.close()
        print(res)
        if 'failed database' in res:
            logger.error(f'failed : insert {schema}.{table}')
            logger.exception(error)
            raise Exception(error)
        return res

def get_sync_data(query_mt4:str = None, query_mt5:str = None, query_archive:str = None, 
                  mt4_servers:List = None, mt5_servers:List = None, do_archive:bool = False, 
                  start_time = datetime(1970, 1, 1, 0, 0 ,0), end_time = datetime.today()):
    df = []
    print("get_sync_data")
    # MT4
    if mt4_servers is not None:
        for server in mt4_servers:
            # print(f"{server}")
            query = query_mt4.format(start_time = start_time, end_time = end_time)
            temp = get_data(query, db = 'mt4', schema= server)
            df.append(temp)
    # Archive
    if do_archive:
        archive_servers = archive_table_mapping[archive_table_mapping.mt4_server.isin(mt4_servers)]['archive_server']
        archive_servers = archive_servers.unique().tolist()
        temp = get_archive_data(query_archive, archive_servers, start_time, end_time)
        df.append(temp)
    # MT5
    if mt5_servers is not None:
        for server in mt5_servers:
            query = query_mt5.format(start_time = start_time, end_time = end_time)
            temp = get_data(query, db = 'mt5_with_archive', schema= server, db_type= 'maria_db')
            df.append(temp)
    df = pd.concat(df)
    df = df.drop_duplicates()
    return df 

def get_archive_data(query_archive:str = None, archive_servers:List = None, 
                     start_time = datetime(1970, 1, 1, 0, 0, 0), end_time = datetime.today()):
    """return data from archive_new database"""
    df = []
    for archive_server in archive_servers:

        query = query_archive.format(archive_server = archive_server, start_time = start_time, end_time = end_time)
        temp = get_data(query, db = 'archive_new', schema= 'archive_db')
        df.append(temp)
    
    df = pd.concat(df)
    
    if 'archive_server' in df.columns and 'group' in df.columns:
        df = df.merge(archivedb_to_server, on = 'archive_server', how = 'left')
        df = df.assign(server = lambda x: np.where(x.group.str.contains('MXT'), 'mxtreport', 
                                     np.where(x.group.str.contains('VT') & x.archive_server.eq('mxt'), 'vtm1report', x.mt4_server)))
        df = df.drop(columns = ['mt4_server', 'archive_server'])
    if 'currency' in df.columns and 'group' in df.columns:
        # 因新歸檔庫user表currency遺失，遺失的currency使用group替代
        df= df.assign(currency =  lambda x :np.where(pd.isna(x.currency) | x.currency.eq(""), x.group.str[-3:], x.currency))
    
    return df 