import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator

import boto3
import psycopg2

host = "redshift-cluster-1.cgpttgasm2sh.us-east-1.redshift.amazonaws.com"
dbname = "dev"
user = "awsuser"
password = "Gogie1234"
port = "5439"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

def _create_tables():


    table_drop_hr_emp = "DROP TABLE IF EXISTS hr_emp"


    table_create_hr_emp = """ 
        CREATE TABLE IF NOT EXISTS hr_emp (
            age text,
            attrition text,
            bsn_travel	text,
            dailyrate text,
            department	text,
            dist_fromhome	text,
            education	text,
            edu_field	text,
            emp_count	text,
            emp_number	text,
            env_satisfaction	text,
            gender	text,
            hourlyrate	text,
            jobinvolvement	text,
            joblevel	text,
            jobrole	text,
            jobsatisfaction	text,
            maritalstatus	text,
            monthlyincome	text,
            monthlyrate	text,
            numcompaniesworked	text,
            over18	text,
            overtime	text,
            percentsalaryhike	text,
            performancerating	text,
            relationshipsatisfaction	text,
            standardhours	text,
            stockoptionlevel	text,
            totalworkingyears	text,
            trainingtimeslastyear	text,
            worklifebalance	text,
            y_atcompany	text,
            y_incurrentrole	text,
            y_sincelastpromotion	text,
            y_withcurrmanager text
        )
    """

    create_table_queries = [
        table_drop_hr_emp,
        table_create_hr_emp,
    ]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()




def _load_tables():
    copy_table = """
    COPY hr_emp FROM 's3://gg-capstone/hr-employee-attrition.csv'
    ACCESS_KEY_ID 'ASIAQUDH5D6C2EVF2LMB'
    SECRET_ACCESS_KEY 'hxqkDdEY1zZrecL2ZN6pgN25qguLhfZELCbBD+bS'
    SESSION_TOKEN 'FwoGZXIvYXdzEDoaDD+EQXWZu4Q9iEq+riLNAftqOMesNC+uiF8uR3XA5cEYQ1CtIGbbcxJ5xAId0rjwHL4FXo3FQWVuthg91PQ07m9M8Kym+GVICtNU1ZrXyOV5jzBsCXLrSY1GWRzSjH7KN8xbgfRT7UhrKd8CEry58cm/vhtfnluRqMgidxb5+3T3BUwUohkOr+ZfPVxPums10lIz1TAEdqJWjolSHu/Faymx418m0xCGcl8iR/lGulj1p3tyf5/DVkFfTx+Vm8++6ihuzpQ0e66CLBzBu8yKnahlrMb4CK/xZ5WiMAEo/6qCnQYyLXQFxlqckw4MWwUWOj6KuRKA57HPlabzMgeRDLvwBl/O9PB4D8ZVdVYC0Xj1rQ=='
    CSV
    DELIMITER ',' 
    IGNOREHEADER 1 
    """
    
    copy_table_queries = [
        copy_table
    ]

    for cop in copy_table_queries:
        cur.execute(cop)
        conn.commit()



with DAG(
    "etl",
    start_date=timezone.datetime(2022, 12, 19),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    create_tables = PythonOperator(
        task_id = "create_tables",
        python_callable = _create_tables,
    )
    
    load_tables = PythonOperator(
        task_id="load_tables",
        python_callable = _load_tables,
    )


    # [get_files, create_tables] >> process
    create_tables >> load_tables

