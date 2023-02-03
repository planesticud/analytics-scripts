import os
import logging
import logging.config
import yaml
import pandas as pd
import calendar
from datetime import  timedelta, date
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


def get_stats(type_stat, stats_table):
    time_now = calendar.timegm((date.today()-timedelta(days=180)).timetuple())
    query_stats = f"select courseid,\
                            timeend,\
                            stattype,\
                            count(userid) total_users,\
                            sum(statsreads) as statsreads,\
                            sum(statswrites) as statswrites\
                            from {stats_table}\
                            where timeend>{time_now}\
                            group by 1, 2, 3"
    data_stats = pd.DataFrame(run_query(query_stats, DB_MYSQL), columns=["courseid", "timeend", "stattype", "total_users", "statsreads", "statswrites"])
    data_stats["type_stat"]  = type_stat
    return data_stats

def get_courses():
    course_query = f"select id, fullname, category_lvl1 from courses_planestic_analytics "
    courses = pd.DataFrame(run_query(course_query, "aulasmetricas", "psql"), columns=["id", "course_name", "main_category"])
    courses['id'] = courses['id'].astype(int)
    return courses

def complement_stats(data_stats_month, data_stats_week, courses):
    data_stats_ok = pd.concat([data_stats_month,data_stats_week])

    data_stats_merge = pd.merge(data_stats_ok, courses, left_on="courseid", right_on='id', how="left")
    data_stats_merge.sort_values(["statsreads", "statsreads"])
    for dates in ['timeend']:
        data_stats_merge[dates] =  pd.to_datetime(data_stats_merge[dates],unit='s')
    return data_stats_merge

def compose_table(data_stats_merge, new_db_name):
    run_query(f"DROP TABLE IF EXISTS {new_db_name}", "aulasmetricas", "psql")
    columns_str = ""
    for col in data_stats_merge.columns:
        if "time" in col:
            columns_str += f"{col} TIMESTAMP, "
        elif col in ["total_users",     "statsreads", "statswrites"]:
            columns_str += f"{col} bigint, "
        else:
            columns_str += f"{col} VARCHAR, "

    run_query(F"CREATE TABLE {new_db_name}\
                ({columns_str[:-2]})",  "aulasmetricas", "psql")

def insert_stats_data(data_stats_merge, new_db_name):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}"
                       .format(user=USER_PSQL,
                               pw=PASS_PSQL,
                               host=HOST_PSQL,
                               db="aulasmetricas"))

    data_stats_merge.to_sql(f'{new_db_name}', con = engine, if_exists = 'append', chunksize = 1000, index=False, schema="aulasschema")

def main():
    logger.info("Get Monthly  Data")
    stats_month = get_stats(type_stat="monthly", stats_table="mdl_stats_user_monthly")
    logger.info("Get Weekly  Data")
    stats_week = get_stats(type_stat="weekly", stats_table="mdl_stats_user_weekly")
    logger.info("Get Courses Data")
    courses = get_courses()
    logger.info("Compose Stats Data")
    stats_complete = complement_stats(stats_month, stats_week, courses)   
    logger.info("Get Ready Stats Analitys Table")
    compose_table(stats_complete, "stats_planestic_analytics")
    logger.info("Insert Data")
    insert_stats_data(stats_complete, "stats_planestic_analytics")

if __name__ == "__main__":
    with open('config/logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    logger.info("Start Process")
    main()
    logger.info("Process Finished Successfully")