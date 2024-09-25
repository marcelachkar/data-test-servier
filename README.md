# Servier Coding Test

## I) Python et Data Engineering

##  Environment

- Python 3
- Pyspark

## Execution of Jobs

### 1- Data Pipeline (on Terminal)
```python
python3 -m pip install pyspark
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
python3 -m unittest discover
```


## Pour Aller plus loin (Q/A)

#### Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?
```text
Afin de gérer de grosses volumétries de données, nous avons développé le projet en utilisant Pyspark pour de mettre
en place une base de code qui pourra être paralellisée et optimisée facilement.
```
#### Pourriez-vous décrire les modifications qu’il faudrait apporter, s’il y en a, pour prendre en considération de telles volumétries ?
```text
- Ingérer les données en chunk (pour aller plus loins utiliser des fichiers de type parquet) 
- Analyser le code pour utiliser la méthode "parallelize" aux endroits les plus pertinents
- Analyser les requêtes SQL et les optimiser afin d'utiliser le moins de mémoire possible et d'être le plus efficace
- Créer un DAG Airflow comme ci-dessous pour lancer le projet:
```
```python
SparkSubmitOperator(
		application = 'main.py' ,
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=dag_spark
		)
```

## II) SQL

The queries are located in the folder SQL.