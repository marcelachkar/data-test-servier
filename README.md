# Servier Coding Test
This project is the source code for the Servier coding test.
In this python project, we create a data pipeline that ingests data of drugs, pubmed articles and clinical trials
then transforms this data into an exploitable model, in order to answer multiple needs:
- Extracting a liaison graph between the different drugs and their mentioned journals
- Extracting the name of the newspaper that mentions the most different drugs 
- Finding the set of drugs mentioned by the same journals referenced by scientific publications but not clinical trials
## Project structure

```python
.
├── README.md
├── data
│   ├── input
│   │   ├── clinical_trials.csv
│   │   ├── drugs.csv
│   │   ├── pubmed.csv
│   │   └── pubmed.json
│   └── result
│       ├── liaison_graph
│       ├── pubmed_drugs
│       └── top_journal
├── main.py
├── requirements.txt
├── sql
│   ├── first_part.sql
│   └── second_part.sql
├── src
│   ├── configs
│   │   └── etl_config.json
│   ├── dependencies
│   │   ├── logging.py
│   │   └── spark.py
│   ├── helpers
│   │   ├── clean.py
│   │   ├── constants.py
│   │   ├── input.py
│   │   ├── job_helper_sql.py
│   │   ├── reader.py
│   │   └── schemas.py
│   └── jobs
│       ├── job_liaison_graph.py
│       ├── job_pubmed_drugs.py
│       └── job_top_journal.py
└── tests
    ├── test_clean.py
    ├── test_input.py
    ├── test_liaison_graph.py
    ├── test_pubmed_drugs.py
    └── test_top_journal.py

```
## I) Python et Data Engineering

##  Environment Requirements

- Python 3
- Pyspark

## Assumptions and cleansing
- we assume that the files provided are conform to the standard of CSV and JSON. in order to garantee this, when being ready for PROD, we can add some pre-processing to the files
- we put in place a process to have the column names in the input files all aligned
- if a row's journal is empty, we skip it since it is not relevant for us in the liaison graph
- we assume that there is one top journal, if needed to have tied top journals we will need to adapt our code
- for the pubmed, we are accepting both JSON and CSV files and loading them together
- we assume that the user of this project is running the project using the described commands below. to make our project fault-proof, we could add exception handlers in case of wrong input.


## Execution of Jobs
! please exectute the commands in the following order to have a liaison graph 
before executing the rest of the project

### Installation of Requirements and packaging
```python
python3 -m pip install pyspark
```

### 1- Data Pipeline (on Terminal)
```python
# to launch job for drug graph
python3 main.py liaison_graph
```

### 2- ad-hoc Bonus (on Terminal)
```python
# to launch job top_journals (bonus 1)
python3 main.py top_journals

# to launch job pubmed_drugs (bonus 2)
python3 main.py pubmed_drugs <drug_name>
```

### 3- Test

```python
python3 -m unittest discover tests
```

## How to be Ready for PROD

To be ready to deploy in Production, we would need to add some wrapping to the project:
- add a tool for dependency and packaging management like Poetry
- create multiple branches: develop, staging, prod. each branch will be assigned to an environment.
in order to add a new feature, you would need to create a Pull request with specified guidelines and
directives, then push your code to the develop branch that will trigger the deployment in the associated
environment. the staging branch will be integration testing with the Business, where we have a bigger pool 
of test data available. Lastly, we would deploy on the prod branch once it is validated by both the Engineers
and the Business, using releases and tagging each deployment
- add a file that will manage the environment variables (depending on where we are deploying)
- add a versioning system and a release management strategy
- in order to run the following code on Airflow, please use this code:
```python
SparkSubmitOperator(
		application = 'main.py' ,
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=dag_spark
		)
```

## Pour Aller plus loin (Q/A)

#### Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?
Afin de gérer de grosses volumétries de données, nous avons développé le projet en utilisant Pyspark pour de mettre
en place une base de code qui pourra être paralellisée et optimisée facilement.
#### Pourriez-vous décrire les modifications qu’il faudrait apporter, s’il y en a, pour prendre en considération de telles volumétries ?
- Ingérer les données en chunk (pour aller plus loins utiliser des fichiers de type parquet) 
- Analyser le code pour utiliser la méthode "parallelize" aux endroits les plus pertinents
- Analyser les requêtes SQL et les optimiser afin d'utiliser le moins de mémoire possible et d'être le plus efficace
- Créer un DAG Airflow pour lancer le projet et suivre les étapes de ready for prod


## II) SQL

The queries are located in the folder SQL.