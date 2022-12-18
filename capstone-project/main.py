import boto3
import psycopg2

aws_access_key_id = "ASIAQUDH5D6CTJNQAXP4"
aws_secret_access_key = "JzRPAQl6U3Y7pRaLhSyDdvEJOhpTNOxapXZ415Rl"
aws_session_token = "FwoGZXIvYXdzEBoaDMGwYgit1BW7sWh5KiLNATWqqoLeUAh2C6dVLuqgLnnqPzob08zrbRtqqZjMQeAlMep/C+diuZIvdyYMG3AJvNKfljZ57QKofJEMVPbvQcC0dQtb5m6/YTB4ssPg0c6IEEZ5jWrDk/mHqUyp4ycX7eY9VsH31apgW5lxYPp2cATO9sW/4bna62VYQOO/+MpKZdN/w67uaOEha1YA4Whic4flp6/9sZov+vIJES2qhV9tgcaFAWp01ZN1U8oDiVWksA0DahI/NhdnXsq6QbyXiBkZ774NS6oIB1CCeEMo0Zr7nAYyLT2qPm8sj3JUa8ZkywoLUOaTUsfgTOqgpsruVRrgi2YPr1B51wSvkkzBnCNcGw=="


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
    """
]

copy_table_queries = [
    """
    COPY hr_emp FROM 's3://gg-capstone/hr-employee-attrition.csv'
    ACCESS_KEY_ID 'ASIAQUDH5D6CTJNQAXP4'
    SECRET_ACCESS_KEY 'JzRPAQl6U3Y7pRaLhSyDdvEJOhpTNOxapXZ415Rl'
    SESSION_TOKEN 'FwoGZXIvYXdzEBoaDMGwYgit1BW7sWh5KiLNATWqqoLeUAh2C6dVLuqgLnnqPzob08zrbRtqqZjMQeAlMep/C+diuZIvdyYMG3AJvNKfljZ57QKofJEMVPbvQcC0dQtb5m6/YTB4ssPg0c6IEEZ5jWrDk/mHqUyp4ycX7eY9VsH31apgW5lxYPp2cATO9sW/4bna62VYQOO/+MpKZdN/w67uaOEha1YA4Whic4flp6/9sZov+vIJES2qhV9tgcaFAWp01ZN1U8oDiVWksA0DahI/NhdnXsq6QbyXiBkZ774NS6oIB1CCeEMo0Zr7nAYyLT2qPm8sj3JUa8ZkywoLUOaTUsfgTOqgpsruVRrgi2YPr1B51wSvkkzBnCNcGw=='
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