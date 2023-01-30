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

def get_user_data():
    role_table_name = "mdl_role"
    enrol_user_table_name = "mdl_user_enrolments"
    user_table_name = "mdl_user"
    enrol_table_name = "mdl_enrol"
    query_users = f"select\
                    u.id,\
                    u.confirmed,\
                    u.deleted,\
                    u.suspended,\
                    u.mnethostid,\
                    u.idnumber,\
                    u.username,\
                    u.firstname,\
                    u.lastname,\
                    u.email,\
                    u.firstaccess,\
                    u.lastaccess,\
                    u.lastlogin,\
                    u.currentlogin,\
                    u.timecreated,\
                    u.timemodified,\
                    eu.status,\
                    eu.enrolid,\
                    eu.timestart,\
                    eu.timeend,\
                    eu.modifierid,\
                    eu.timecreated,\
                    eu.timemodified,\
                    en.enrol,\
                    en.status,\
                    en.courseid,\
                    en.name,\
                    en.enrolperiod,\
                    en.enrolstartdate,\
                    en.enrolenddate,\
                    en.roleid,\
                    r.shortname\
                from {user_table_name} as u\
                left join {enrol_user_table_name} as eu on u.id=eu.userid\
                left join {enrol_table_name} as en on eu.enrolid=en.id\
                left join {role_table_name} as r on en.roleid=r.id\
                where u.deleted<>1 and u.suspended<>1\
                "
    user_columns = ["id", "confirmed", "deleted", "suspended", "mnethostid", "idnumber", "username", "firstname", "lastname", 
            "email", "firstaccesstime", "lastaccesstime", "lastlogintime", "currentlogintime", "timecreated",
            "timemodified", "enrol_user_status", "enrol_user_enrolid",  "enrol_user_timestart", "enrol_user_timeend",
            "enrol_user_modifierid", "enrol_user_timecreated", "enrol_user_timemodified", "enrol_enrol", "enrol_status",
            "enrol_courseid", "enrol_name", "enrol_period", "enrol_startdate", "enrol_enddate", "enrol_roleid", "role_name"]
    
    users = pd.DataFrame(run_query(query_users, DB_MYSQL), columns=user_columns, dtype='str')
    users  = users.fillna({"role_name":"no_enroled", "enrol_courseid":"0"})
    users["enrol_courseid"] = users["enrol_courseid"].astype(float).astype(int)

    for col in users.columns:
        if "time" in col:
            users[col] =  pd.to_datetime(users[col],unit='s')
            users.loc[users[col] == '1970-01-01 00:00:00 ', col] = pd.NaT

    return users
     
def compose_table(users):
    run_query("DROP TABLE IF EXISTS users_planestic_analytics", "aulasmetricas", "psql")
    columns_str = ""
    for col in users.columns:
        if "time" in col:
            columns_str += f"{col} TIMESTAMP, "
        else:
            columns_str += f"{col} VARCHAR, "
    run_query(F"CREATE TABLE users_planestic_analytics\
                ({columns_str[:-2]})",  "aulasmetricas", "psql")

def insert_user_data(users):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}"
                        .format(user=USER_PSQL,
                                pw=PASS_PSQL,
                                host=HOST_PSQL,
                                db="aulasmetricas"))
    users.to_sql('users_planestic_analytics', con = engine, if_exists = 'append', chunksize = 1000, index=False, schema="aulasschema")

def main():
    logger.info("Get Users Data")
    users = get_user_data()
    logger.info("Get Ready Users Analitys Table")
    compose_table(users)
    logger.info("Insert Data")
    insert_user_data(users)

if __name__ == "__main__":
    with open('config/logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    logger.info("Start Process")
    main()
    logger.info("Process Finished Successfully")