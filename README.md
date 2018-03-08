Analyse de données - EnerNOC GreenButton 

### Commandes Hadoop

```
hadoop fs -mkdir -p /group3a/raw/v1/csv
hadoop fs -mkdir -p /group3a/raw/v1/meta

hadoop fs -put ./csv/*.csv /group3a/raw/v1/csv
hadoop fs -put ./meta/all_sites.csv /group3a/raw/v1/meta

hadoop fs -chown hive:hdfs /group3a/raw/v1/csv
hadoop fs -chown hive:hdfs /group3a/raw/v1/meta

```

### HIVE DDL

IN REWORK

### HIVE DML

IN REWORK
