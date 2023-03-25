import os
import logging
import logging.config
import yaml
import pandas as pd
import mysql.connector
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
USER_MYSQL = os.getenv("USER_MYSQL")
USER_PSQL = os.getenv("USER_PSQL")
HOST_MYSQL = os.getenv("HOST_MYSQL")
HOST_PSQL = os.getenv("HOST_PSQL")
PASS_MYSQL =  os.getenv("PASS_MYSQL")
PASS_PSQL = os.getenv("PASS_PSQL")
DB_MYSQL =  os.getenv("DB_MYSQL")

def run_query(query='', database="", engine="mysql"):
    if database != "":
        if engine=="mysql":
            conn =  mysql.connector.connect(user=USER_MYSQL, password=PASS_MYSQL,
                                host=HOST_MYSQL,
                                database=database)
        elif engine=="psql":
            conn = psycopg2.connect(user=USER_PSQL, password=PASS_PSQL, host=HOST_PSQL,
                                port=5432, database=database, options="-c search_path=dbo,aulasschema")
    else:
        if engine=="mysql":
            conn =  mysql.connector.connect(user='root', password='example',
                              host='drone.planestic.udistrital.edu.co' )   
        elif engine=="psql":
            print("Postgresql debe tener asignada una base de datos")
    if engine=="psql":
        cursor = conn.cursor()
    else:
        cursor = conn.cursor(buffered=True)
    cursor.execute(query)
    if query.upper().startswith('SELECT'):
        data = cursor.fetchall()
    elif query.upper().startswith('INSERT'):
        conn.commit()
        data = cursor.fetchall()
    elif query.upper().startswith('SHOW'):
        
        data = cursor.fetchall()
    else:
        conn.commit()
        data = None
    cursor.close()
    conn.close()
    return data


def get_bbb_data():
    query_stats = f"SELECT\
                    FROM_UNIXTIME(timecreated,'%Y-%m-%d %H:%i:%s') AS time,\
                    component,\
                    eventname,\
                    case\
                    when contextlevel = 10 then 'system'\
                    when contextlevel = 50 then 'course'\
                    when contextlevel = 30 then 'user'\
                    when contextlevel = 40 then 'category'\
                    when contextlevel = 70 then 'activity'\
                    when contextlevel = 80 then 'block'\
                    end as event_context,\
                    action,\
                    origin,\
                    ip,\
                    courseid,\
                    userid\
                    FROM mdl_logstore_standard_log\
                    where component = 'mod_bigbluebuttonbn'"
    data_bbb = pd.DataFrame(run_query(query_stats, DB_MYSQL), columns=["time", "component", "eventname", "event_context", "action", "origin", "ip_address"])
    return data_bbb


def compose_table(data_bbb, new_db_name):
    #run_query(f"DROP TABLE IF EXISTS {new_db_name}", "aulasmetricas", "psql")
    columns_str = ""
    for col in data_bbb.columns:
        if "time" in col:
            columns_str += f"{col} TIMESTAMP, "
        elif col in ["total_users",     "statsreads", "statswrites"]:
            columns_str += f"{col} bigint, "
        else:
            columns_str += f"{col} VARCHAR, "

    run_query(F"CREATE TABLE IF NOT EXISTS {new_db_name}\
                ({columns_str[:-2]})",  "aulasmetricas", "psql")

def insert_stats_data(data_bbb, new_db_name):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}"
                       .format(user=USER_PSQL,
                               pw=PASS_PSQL,
                               host=HOST_PSQL,
                               db="aulasmetricas"))

    data_bbb.to_sql(f'{new_db_name}', con = engine, if_exists = 'append', chunksize = 1000, index=False, schema="aulasschema")

def main():
    logger.info("Get BBB  Data")
    bbb_data = get_bbb_data()
    compose_table(bbb_data, "bbb_planestic_analytics")
    insert_stats_data(bbb_data, "bbb_planestic_analytics")

if __name__ == "__main__":
    with open('config/logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    logger.info("Start Process")
    main()
    logger.info("Process Finished Successfully")