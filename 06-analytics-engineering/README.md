# Analytics Engineering

Create a dbt project

```sh
dbt init
```

Edit the dbt profiles

```sh
code ~/.dbt/profiles.yml
```

```yml
jaffle:
  outputs:

    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: public

    prod:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: prod

  target: dev
```

Test dbt connection

```sh
cd jaffle
dbt debug
```

You should see "All checks passed!".

![allcheckpassed](https://user-images.githubusercontent.com/111683692/204131670-557564ef-cc32-4443-9ed2-5a87867348b7.jpg)


To create models

```sh
dbt run
```

Check data on SQLPad
![database](https://user-images.githubusercontent.com/111683692/204131739-a233beda-a7c6-45ad-a747-bee2d9b83a45.jpg)


To test models

```sh
dbt test
```

To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve --no-browser
```
Data Catalog

![dbtdocs](https://user-images.githubusercontent.com/111683692/204131813-a6c0a756-98f4-40a6-b810-d47beda5c81f.jpg)

View Lineage Graph

![lineagegraph](https://user-images.githubusercontent.com/111683692/204131862-e9cbdaf1-a6f1-4ab1-bf84-862958510d61.jpg)

