#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as f

# Создаем сессию spark
spark = SparkSession.builder.appName(
    "sbersparktask2").master("yarn").getOrCreate()

#  Создаем схему данных
schema = StructType(fields=[
    StructField("follower_id", StringType()),
    StructField("user_id", StringType())
])

# Читаем данные
df = spark.read.format("csv").option("sep", "\t").schema(
    schema).load("/data/twitter/twitter_sample.txt")

# Параметры поиска
START_NODE = '34'
TARGET_NODE = '12'

# Стартовый узел (34)
current_level = spark.createDataFrame(
    [(START_NODE, [START_NODE])],
    schema="node_id STRING, path ARRAY<STRING>"
)

# Алгоритм "поиск в ширину"
while True:

# Присоединяем следующий уровень узлов:
    next_node = current_level.alias("cur") \
        .join(df.alias("d"), f.col("cur.node_id") == f.col("d.follower_id"), "inner") \
        .select(
            f.col("d.user_id").alias("node_id"),
            f.concat(f.col("cur.path"), f.array(
                f.col("d.user_id"))).alias("path")
    ).cache()

# Проверяем, есть ли целевой узел
    is_target = next_node.filter(f.col("node_id") == TARGET_NODE).head()

    if is_target:
        result_path = is_target["path"]
        print(",".join(reversed(result_path)))
        break

# Удаляем дубли, оставляем только первый найденный путь к узлу
    current_level = next_node.dropDuplicates(["node_id"]).coalesce(2)
