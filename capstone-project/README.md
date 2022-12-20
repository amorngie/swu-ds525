# Capstone Project

## Run command
```sh
cd capstone-project
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```
## Upload data to S3
```sh
python main.py
```
## Create table and load data from S3 to Redshift (Automating pipelines using Apache Airflow)
Running Airflow

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

```sh
docker-compose up
```
Create connection to Redshift
![Screenshot (332)](https://user-images.githubusercontent.com/111683692/208593165-87b578c9-fbb8-4e35-9be4-44ca09037c66.png)

Trigger Dag
![Screenshot (332)](https://user-images.githubusercontent.com/111683692/208593310-cd59a740-6e2d-41dc-b293-f6dbcd4d3a01.png)


## Use dbt for data modeling and data tranforming
Create a dbt project

```sh
dbt init
```

Test dbt connection

```sh
cd dbt_empdata
dbt debug
```

You should see "All checks passed!".
![Screenshot (345)](https://user-images.githubusercontent.com/111683692/208593700-713e8c1f-0b79-48b4-b62e-f0e19596973a.png)



To create models

```sh
dbt run
```

Check data on Redshift
![Screenshot (350)](https://user-images.githubusercontent.com/111683692/208593524-b0f20509-af77-42bc-8d22-5a0aa2ecc672.png)


To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve --no-browser
```
