import boto3
import psycopg2

aws_access_key_id = "ASIAQUDH5D6CVMUIV33L"
aws_secret_access_key = "q5Hwv+E9rxKg5qO6E4T/ZDf+RiIEXSsJeTcGE/s6"
aws_session_token = "FwoGZXIvYXdzECMaDJxSPhWHwOQTzSyQWyLNAbGxv4oqaMvb4dMplGmkq4TzNAE3Pl7o2bJiA3Rl1wOG1W0owg7KNUqzgEXNQXnZ1UYd00pFwU5tbWcEC9mU7YUWNr5GGYHOTNetp2WvZ9YsFabj7MxuQ+Rrx4oHLdcsQFA0VnRacwywf+VAJmGbImxnHeMyE0bEkoxut/Akr5urlsPMBAMfS3pwvdlEd6cmXtOQZQ5yShY8Tc+oXk5ajWGtmKQtLnHkFe0HiDo1FOkL/A7VLi10XMZERfw4hqYDGlLOmawzT/3MXj5YUQYo2qT9nAYyLUqw4ZHXUVsEXzYPA/YMEM7qJcO/I09HzxIzDLyI+8oDPnA+eORY9fwNWJlqFw=="


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