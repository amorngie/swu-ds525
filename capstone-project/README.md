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

![Picture7](https://user-images.githubusercontent.com/111683692/208725189-551b3b8c-dcd4-4ca6-8498-9247c525cccd.png)



Trigger Dag

![Picture8](https://user-images.githubusercontent.com/111683692/208725208-51097aae-7473-470e-bd72-f65bc7f29d56.png)





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

![Picture9](https://user-images.githubusercontent.com/111683692/208725128-e31210be-21a1-4731-b311-7d2ee54e5bf1.png)






To create models

```sh
dbt run
```

Check data on Redshift

![Picture10](https://user-images.githubusercontent.com/111683692/208725250-766a9964-4347-4389-b7b1-6a23949bfa73.png)






To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve --no-browser
```

![Picture6](https://user-images.githubusercontent.com/111683692/208725275-a5f82801-8fde-4ca0-bc28-cd251673ba05.png)



