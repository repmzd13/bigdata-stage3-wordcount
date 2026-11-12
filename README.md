# Big Data Integration – Stage 3: Word counter with Hadoop and Spark

Course: Big Data Integration (203008077), Universidad Nacional Abierta y a Distancia (UNAD).

This repository contains the code used in the practical component of Stage 3. The same word count is run in two environments:

1. **Local Hadoop (MapReduce)** on a Linux virtual machine in pseudo-distributed mode.
2. **Databricks (Spark and Spark SQL)** in Databricks Free Edition.

## Files

| File | Description |
|---|---|
| `WordCount.java` | MapReduce job (mapper, combiner and reducer) for Hadoop 2.x. |
| `Annex_1_Aplication.txt` | Input data: 300 lines, one word per line. |
| `WordCount_Databricks.py` | Databricks notebook (source format) with the ETL flow, Spark SQL queries and charts. |
| `WordCount_Databricks.html` | HTML export of the executed notebook. |

## Run in Hadoop

```bash
export HADOOP_CLASSPATH=$(hadoop classpath)
hadoop fs -mkdir -p /WordCountTutorial/Input
hadoop fs -put Annex_1_Aplication.txt /WordCountTutorial/Input
mkdir -p tutorial_classes
javac -classpath ${HADOOP_CLASSPATH} -d tutorial_classes WordCount.java
jar -cvf firstTutorial.jar -C tutorial_classes/ .
hadoop jar firstTutorial.jar WordCount /WordCountTutorial/Input/Annex_1_Aplication.txt /WordCountTutorial/Output
hadoop fs -cat /WordCountTutorial/Output/*
```

## Run in Databricks

1. In the workspace, go to **Workspace > Import** and select `WordCount_Databricks.py`.
2. Run the first SQL cell to create the volume `workspace.default.bigdata`.
3. Upload `Annex_1_Aplication.txt` to that volume (**Catalog > workspace > default > bigdata > Upload to this volume**).
4. Run all cells.

## Result

| Word | Count |
|---|---|
| Vision | 66 |
| Assistants | 62 |
| Robotics | 61 |
| Predictive | 54 |
| Speech | 18 |
| Recommendation | 16 |
| Processing | 8 |
| Learning | 5 |
| Recognition | 4 |
| Automation | 2 |

Total: 296 words, 10 distinct terms. Both environments return the same counts.
