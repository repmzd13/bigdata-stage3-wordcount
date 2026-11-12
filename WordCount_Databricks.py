# Databricks notebook source
# MAGIC %md
# MAGIC # Word counter with Spark and Spark SQL in Databricks
# MAGIC **Course:** Big Data Integration (203008077), UNAD. Stage 3, Practical component.
# MAGIC
# MAGIC This notebook repeats the Hadoop MapReduce word count of Part 1 in a cloud environment. It follows an ETL flow:
# MAGIC
# MAGIC 1. **Extract:** read `Annex_1_Aplication.txt` from a Unity Catalog volume (the cloud equivalent of an HDFS directory).
# MAGIC 2. **Transform:** clean each line, split it into words and count them with PySpark (the map and reduce steps).
# MAGIC 3. **Load:** save the result as a Delta table.
# MAGIC 4. **Analyze:** query the table with Spark SQL and visualize the results.
# MAGIC
# MAGIC Environment: Databricks Free Edition (serverless compute). Community Edition was retired on January 1, 2026, so Free Edition is used instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup
# MAGIC Before running the notebook, create the volume and upload the file:
# MAGIC 1. Run the next cell to create the volume `workspace.default.bigdata`.
# MAGIC 2. Go to **Catalog > workspace > default > bigdata** and click **Upload to this volume**. Select `Annex_1_Aplication.txt`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.bigdata;

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "bigdata"
FILE_NAME = "Annex_1_Aplication.txt"

input_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{FILE_NAME}"
output_table = f"{CATALOG}.{SCHEMA}.word_count_results"

print("Input file:", input_path)
display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract
# MAGIC `spark.read.text` creates a DataFrame with one row per line and a single column called `value`. This plays the same role as `TextInputFormat` in Hadoop, where each line is one input record for the mapper.

# COMMAND ----------

raw_df = spark.read.text(input_path)

print("Lines read:", raw_df.count())
raw_df.printSchema()
display(raw_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform
# MAGIC Steps:
# MAGIC - Remove carriage returns (`\r`) left by Windows line endings and trim spaces.
# MAGIC - Split each line on whitespace and use `explode` to get one row per word. This is the equivalent of the mapper emitting (word, 1).
# MAGIC - Drop empty strings, which come from the empty lines of the file.
# MAGIC - Group by word and count. This is the equivalent of shuffle + reduce.

# COMMAND ----------

clean_df = raw_df.select(
    F.trim(F.regexp_replace(F.col("value"), r"\r", "")).alias("line")
)

words_df = (
    clean_df
    .select(F.explode(F.split(F.col("line"), r"\s+")).alias("word"))
    .filter(F.col("word") != "")
)

print("Empty lines discarded:", clean_df.filter(F.col("line") == "").count())
print("Total words:", words_df.count())

word_counts_df = (
    words_df
    .groupBy("word")
    .agg(F.count("*").alias("count"))
    .orderBy(F.col("count").desc(), F.col("word"))
)

display(word_counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load
# MAGIC The result is saved as a managed Delta table. In Hadoop the result stays as a text file (`part-r-00000`) in HDFS; here it becomes a table that can be queried with SQL, keeps a version history and is registered in Unity Catalog.

# COMMAND ----------

(
    word_counts_df
    .withColumn("processed_at", F.current_timestamp())
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table)
)

print("Table saved:", output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Analysis with Spark SQL
# MAGIC ### 4.1 Results table, ordered as in the Hadoop output (alphabetical)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT word, count
# MAGIC FROM workspace.default.word_count_results
# MAGIC ORDER BY word;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Ranking and share of each word

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   RANK() OVER (ORDER BY count DESC) AS rank,
# MAGIC   word,
# MAGIC   count,
# MAGIC   ROUND(100 * count / SUM(count) OVER (), 2) AS percentage
# MAGIC FROM workspace.default.word_count_results
# MAGIC ORDER BY rank, word;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Summary statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)              AS distinct_words,
# MAGIC   SUM(count)            AS total_words,
# MAGIC   MAX(count)            AS max_frequency,
# MAGIC   MIN(count)            AS min_frequency,
# MAGIC   ROUND(AVG(count), 2)  AS avg_frequency
# MAGIC FROM workspace.default.word_count_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 The same word count written only in SQL
# MAGIC The whole MapReduce job can also be expressed as one SQL query over the raw file, using `read_files`. The result must be identical to the PySpark version.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT word, COUNT(*) AS count
# MAGIC FROM (
# MAGIC   SELECT explode(split(trim(regexp_replace(value, '\r', '')), '\\s+')) AS word
# MAGIC   FROM read_files('/Volumes/workspace/default/bigdata/Annex_1_Aplication.txt', format => 'text')
# MAGIC )
# MAGIC WHERE word <> ''
# MAGIC GROUP BY word
# MAGIC ORDER BY count DESC, word;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Words that represent at least 10% of the text

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT word, count
# MAGIC FROM workspace.default.word_count_results
# MAGIC WHERE count >= 0.10 * (SELECT SUM(count) FROM workspace.default.word_count_results)
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Visualization

# COMMAND ----------

import matplotlib.pyplot as plt

pdf = spark.table(output_table).orderBy("count").toPandas()

fig, ax = plt.subplots(figsize=(9, 5))
bars = ax.barh(pdf["word"], pdf["count"], color="#3b6ea5")
ax.bar_label(bars, padding=3)
ax.set_title("Word frequency in Annex 1 (Spark on Databricks)")
ax.set_xlabel("Occurrences")
ax.set_ylabel("Word")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verification against the Hadoop result
# MAGIC The counts below are the output of the MapReduce job run in the local virtual machine (Part 1). The cell compares them with the Spark result.

# COMMAND ----------

hadoop_result = {
    "Assistants": 62, "Automation": 2, "Learning": 5, "Predictive": 54,
    "Processing": 8, "Recognition": 4, "Recommendation": 16,
    "Robotics": 61, "Speech": 18, "Vision": 66,
}

spark_result = {r["word"]: r["count"] for r in spark.table(output_table).collect()}

for word in sorted(hadoop_result):
    status = "OK" if spark_result.get(word) == hadoop_result[word] else "DIFFERENT"
    print(f"{word:<15} Hadoop={hadoop_result[word]:>3}  Spark={spark_result.get(word, 0):>3}  {status}")

print("\nBoth environments match:", spark_result == hadoop_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Conclusions
# MAGIC - Spark and Hadoop produce the same counts for the 296 words of the file (10 distinct terms; "Vision" is the most frequent with 66 occurrences, followed by "Assistants" with 62 and "Robotics" with 61).
# MAGIC - In Hadoop the job needed a Java class, compilation, a JAR file, HDFS commands and the HDFS and YARN daemons running. In Databricks the same logic takes a few lines of PySpark or a single SQL query, and the compute is provisioned automatically.
# MAGIC - Hadoop writes the result as a text file; Databricks stores it as a Delta table that can be queried with SQL and visualized in the same notebook.
# MAGIC - For a 300-line file the distributed processing does not make a difference in time. The value of both tools appears with large volumes of data, where Hadoop splits the work in blocks across nodes and Spark keeps intermediate results in memory.
