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


copy_table_queries = [
    """
    COPY hr_emp FROM 's3://gg-capstone/hr-employee-attrition.csv'
    ACCESS_KEY_ID 'ASIAQUDH5D6C6YHJQI4S'
    SECRET_ACCESS_KEY 'dHbip448+z76LkqpuRXNhRMQ67K2ZVYfLmfZ9wKy'
    SESSION_TOKEN 'FwoGZXIvYXdzEB8aDH4dkLTZUfuqpMusuiLNAWCZsaq+vteoS3bBmYI5bB48LpOpbx/IZsCVdeN3qTcWVyAvjUDrociLOkfinlPAamIayJmDoDJ1SxWQozzhd2J1QzOkuYzMC5Tg0E6pwyGkCcBm8yTWpzMOc4Ftgy+k/r1votXenULa4j1KENDHUxk8MwMEYKnPGwZAi8xxcX3zTki9Dl76SEimIXnrA2a60NYRXyuaX7sFIaAn20irkHLd9+O+8Ej6od/2EypGRK2oEuUMTjOAfN4slSF0YmFTSK7oEo9qm6SJznZoegYoprD8nAYyLZhGMtfKo9lZQUTV5DDrmZalHb76d+8kxkCCZm22U3rT1gdKsmpvlxD17eMDIA=='
    CSV
    DELIMITER ',' 
    IGNOREHEADER 1 
    """
]

def _load_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 12, 17),
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

