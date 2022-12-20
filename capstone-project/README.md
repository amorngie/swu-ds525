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

![connectaf](https://user-images.githubusercontent.com/111683692/208722924-60cc152b-a6f2-437f-8f8b-7d451de03027.jpg)

Trigger Dag
![trigger](https://user-images.githubusercontent.com/111683692/208722975-915afb14-ac1d-4c31-adbe-f022381184e9.jpg)




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

![dbtdebug](https://user-images.githubusercontent.com/111683692/208723008-94b58181-d21d-4734-b668-0a0499ae47f4.jpg)



To create models

```sh
dbt run
```

Check data on Redshift

![redshift](https://user-images.githubusercontent.com/111683692/208723049-f9a3caf6-b37b-4234-83af-622d3f5c5a14.jpg)



To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve --no-browser
```

![dbtlineage](https://user-images.githubusercontent.com/111683692/208722295-bc273a3f-4c2f-4184-b19b-d1829746fc68.jpg)

