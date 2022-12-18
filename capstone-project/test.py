import boto3
import psycopg2

aws_access_key_id = "ASIAQUDH5D6CQZ6A73YJ"
aws_secret_access_key = "6CPdIe1JEq8WT9UkXwOwBZTjXSZfAuF27MknypSp"
aws_session_token = "FwoGZXIvYXdzEAsaDH3fOI5clR5oGM/K2SLNAQHn+3Xyv+cwUPR/lWz/vZLf4Ubfuo4kwwYOJQ/q7G/ohBAoPZPpWbkx9fwuY6x9TKsIBQEb2/e5tDvh23eU4SUAIe7AUzXKURpFCKp3kqyKMhyYrzvhMgU0qoq8Ys/pdoOufSxEldwb8v7zoqyCgKudFA6v/DSHGxKqGRpweIgxehAAF0twlsPRuVnkzufFMSseO4aBtV6+0pnwxlvJfdHiNnHHlG3G2rkBQfDzL6jAL/8fpPXMQBR2SHjpIrtOgUxjXeZKZ8cMOmU+vr4onvv3nAYyLaUQG7AXz4iYgP3L45XnCF9BqWcIGCUpVsdGlLCbdEEtZJIuz+zhIFeuWrgCRw=="


s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)
s3.meta.client.upload_file(
    "hr-employee-attrition.csv",
    "gg-capstone",
    "hr-employee-attrition.csv",
)
s3.meta.client.get_object(
    Bucket = "gg-capstone",
    Key = "hr-employee-attrition.csv"
)

drop_table_queries = [
    "DROP TABLE IF EXISTS hr",
    "DROP TABLE IF EXISTS hr_emp"
]

create_table_queries = [
    """
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
    """,
]

copy_table_queries = [
    """
    COPY hr_emp FROM 's3://gg-capstone/hr-employee-attrition.csv'
    ACCESS_KEY_ID 'ASIAQUDH5D6CQZ6A73YJ'
    SECRET_ACCESS_KEY '6CPdIe1JEq8WT9UkXwOwBZTjXSZfAuF27MknypSp'
    SESSION_TOKEN 'FwoGZXIvYXdzEAsaDH3fOI5clR5oGM/K2SLNAQHn+3Xyv+cwUPR/lWz/vZLf4Ubfuo4kwwYOJQ/q7G/ohBAoPZPpWbkx9fwuY6x9TKsIBQEb2/e5tDvh23eU4SUAIe7AUzXKURpFCKp3kqyKMhyYrzvhMgU0qoq8Ys/pdoOufSxEldwb8v7zoqyCgKudFA6v/DSHGxKqGRpweIgxehAAF0twlsPRuVnkzufFMSseO4aBtV6+0pnwxlvJfdHiNnHHlG3G2rkBQfDzL6jAL/8fpPXMQBR2SHjpIrtOgUxjXeZKZ8cMOmU+vr4onvv3nAYyLaUQG7AXz4iYgP3L45XnCF9BqWcIGCUpVsdGlLCbdEEtZJIuz+zhIFeuWrgCRw=='
    CSV
    DELIMITER ',' 
    IGNOREHEADER 1 
    """
]



def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()




def main():
    host = "redshift-cluster-1.cgpttgasm2sh.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Gogie1234"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_tables(cur, conn)

if __name__ == "__main__":
    main()