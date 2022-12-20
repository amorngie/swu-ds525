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

![Picture7](https://user-images.githubusercontent.com/111683692/208723321-c6e5c5db-daad-4d0f-b734-16ef667acd5f.png)


Trigger Dag
![Picture8](https://user-images.githubusercontent.com/111683692/208723341-0541cc33-2da9-4a20-9cc1-63ef9c2fd624.png)





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
![Picture9](https://user-images.githubusercontent.com/111683692/208723366-ae04a125-8080-4ccc-a567-2c10bd4d6dc5.png)





To create models

```sh
dbt run
```

Check data on Redshift
![Picture10](https://user-images.githubusercontent.com/111683692/208723408-7134f6d1-0383-4cf2-a451-9738ea3fe3a3.png)





To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve --no-browser
```
![Picture6](https://user-images.githubusercontent.com/111683692/208723443-d5b4a138-712f-422c-be67-ea85591f8796.png)



