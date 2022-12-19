# Capstone Project

## Run command
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```
## Upload data to S3
```sh
python main.py
```
## Create table and load data from S3 to Redshift (Automating pipelines using Apache Airflow)
```sh
python main.py
```
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


To create models

```sh
dbt run
```

Check data on Redshift


To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve --no-browser
```
