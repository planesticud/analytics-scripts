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
            conn =  mysql.connector.connect(user=USER_MYSQL, password=PASS_MYSQL,
                              host=HOST_MYSQL )   
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

def get_len(serie):
    return len(serie)
def get_levels(df):
    for i in range(1,df['lenght']-1):
        df[f'lvl_{i}'] = int(df['lvl'][i])
    return df

def get_categories():
    category_table_name = "mdl_course_categories"
    category_columns = list((pd.DataFrame(run_query(f"SELECT *\
                                                FROM INFORMATION_SCHEMA.COLUMNS\
                                                WHERE TABLE_SCHEMA ='{DB_MYSQL}' and \
                                                TABLE_NAME = N'{category_table_name}'\
                                                ORDER BY ORDINAL_POSITION")))[3])

    categories  = pd.DataFrame(run_query(f"SELECT * FROM {category_table_name}", DB_MYSQL), columns=category_columns)

    categ_lvl_1 =  categories.copy()
    categories['lvl'] = categories['path'].str.split('/') 
    categories['lenght'] = categories['lvl'].apply(lambda x: len(x))
    for i in range(1,categories['lenght'].max()-1):
        categories[f'lvl_{i}'] ='' 
    categories = categories.apply(get_levels,axis=1)
    categories = pd.merge(categories,categ_lvl_1[['id','name']],left_on=['lvl_1'],right_on=['id'],how='left')
    categories = pd.merge(categories,categ_lvl_1[['id','name']],left_on=['lvl_2'],right_on=['id'],how='left')
    categories =  categories.rename(columns={'id_x':'categoryid','name_x':'category_name','name_y':'category_lvl1','name':'category_lvl2', "visible":"category_visible"})
    categories.drop(columns=['id_y', 'id','sortorder','visibleold','theme'],inplace=True)
    return categories



def get_courses():
    course_table_name = "mdl_course"
    course_columns = list((pd.DataFrame(run_query(f"SELECT *\
                                                FROM INFORMATION_SCHEMA.COLUMNS\
                                                WHERE TABLE_SCHEMA ='{DB_MYSQL}' and \
                                                TABLE_NAME = N'{course_table_name}'\
                                                ORDER BY ORDINAL_POSITION")))[3])
    logger.info(len(course_columns))
    courses  = pd.DataFrame(run_query(f"SELECT * FROM {course_table_name}", DB_MYSQL), columns=course_columns)
    return courses

def complement_courses(courses, categories):
    courses = pd.merge(courses,categories[['categoryid','category_name','lvl_1','lvl_2','category_lvl1','category_lvl2',"depth", "parent", "path", "lenght", "category_visible"]],left_on="category", right_on= 'categoryid',how='left')

    # courses.to_csv("data/courses_fitted.csv",sep='|',index=False)
    for dates in ['timecreated', 'timemodified', 'startdate', 'enddate']:
        courses[dates] =  pd.to_datetime(courses[dates],unit='s')
    courses.drop(columns=['summary', 'summaryformat', "relativedatesmode"],inplace=True)
    return courses

def compose_table(courses, new_db_name):
    run_query(f"DROP TABLE IF EXISTS {new_db_name}", "aulasmetricas", "psql")

    columns_str = ""
    for col in courses.columns:
        if ("time" in col or "date" in col) and col!="showactivitydates":
            columns_str += f"{col} TIMESTAMP, "
        else:
            columns_str += f"{col} VARCHAR, "

    run_query(f"""CREATE TABLE {new_db_name}\
            ({columns_str[:-2]})""", "aulasmetricas", "psql")


def insert_courses_data(courses, new_db_name):
    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}"
                       .format(user=USER_PSQL,
                               pw=PASS_PSQL,
                               host=HOST_PSQL,
                               db="aulasmetricas"))

    courses.to_sql(new_db_name, con = engine, if_exists = 'append', chunksize = 1000, index=False, schema="aulasschema")

def main():
    logger.info("Get Categories  Data")
    categories = get_categories()
    logger.info("Get Courses Data")
    courses = get_courses()
    logger.info("Compose Courses Data")
    courses_complete = complement_courses(courses, categories)   
    logger.info("Get Ready Courses Analitys Table")
    compose_table(courses_complete, "courses_planestic_analytics")
    logger.info("Insert Data")
    insert_courses_data(courses_complete, "courses_planestic_analytics")

if __name__ == "__main__":
    with open('config/logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    logger.info("Start Process")
    main()
    logger.info("Process Finished Successfully")