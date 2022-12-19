import boto3
import psycopg2

aws_access_key_id = "ASIAQUDH5D6C2EVF2LMB"
aws_secret_access_key = "hxqkDdEY1zZrecL2ZN6pgN25qguLhfZELCbBD+bS"
aws_session_token = "FwoGZXIvYXdzEDoaDD+EQXWZu4Q9iEq+riLNAftqOMesNC+uiF8uR3XA5cEYQ1CtIGbbcxJ5xAId0rjwHL4FXo3FQWVuthg91PQ07m9M8Kym+GVICtNU1ZrXyOV5jzBsCXLrSY1GWRzSjH7KN8xbgfRT7UhrKd8CEry58cm/vhtfnluRqMgidxb5+3T3BUwUohkOr+ZfPVxPums10lIz1TAEdqJWjolSHu/Faymx418m0xCGcl8iR/lGulj1p3tyf5/DVkFfTx+Vm8++6ihuzpQ0e66CLBzBu8yKnahlrMb4CK/xZ5WiMAEo/6qCnQYyLXQFxlqckw4MWwUWOj6KuRKA57HPlabzMgeRDLvwBl/O9PB4D8ZVdVYC0Xj1rQ=="


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