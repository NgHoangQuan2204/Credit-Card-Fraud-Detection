from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, FloatType

from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

import time
import logging
import pandas as pd
import pickle

spark = SparkSession.builder \
    .appName("CCT") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

schema = StructType() \
    .add("merchant", DoubleType()) \
    .add("category", DoubleType()) \
    .add("amt", FloatType()) \
    .add("gender", DoubleType()) \
    .add("lat", FloatType()) \
    .add("long", FloatType()) \
    .add("city_pop", DoubleType()) \
    .add("job", DoubleType()) \
    .add("unix_time", DoubleType()) \
    .add("merch_lat", FloatType()) \
    .add("merch_long", FloatType()) \
    .add("is_fraud", DoubleType())
    
source_df = (
    spark \
    .readStream \
    .format('kafka') \
    .options(**{
        'subscribe': 'CCT',
        'startingOffsets': 'latest',
    }) \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .load()
)

value_df = source_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def process_batch(batch_df, batch_id):
    start_time = time.time()
    if not batch_df.isEmpty():
        pdf = batch_df.toPandas()
        pdf['merchant'] = pdf['merchant'].astype("int64")
        pdf['category'] = pdf['category'].astype("int64")
        pdf['gender'] = pdf['gender'].astype("int64")
        pdf['city_pop'] = pdf['city_pop'].astype("int64")
        pdf['job'] = pdf['job'].astype("int64")
        pdf['unix_time'] = pdf['unix_time'].astype("int64")
        pdf['is_fraud'] = pdf['is_fraud'].astype("int64")

        X = pdf[["merchant", "category", "amt", "gender", "lat", "long",
         "city_pop", "job", "unix_time", "merch_lat", "merch_long"]]
        

        y_true = pdf["is_fraud"]

        # with open('D:/Project/Credit-Card-Fraud-Detection/notebooks/models/svc.pkl', 'rb') as f:
        #     svc = pickle.load(f)
        # with open('D:/Project/Credit-Card-Fraud-Detection/notebooks/models/lr.pkl', 'rb') as f:
        #     lr = pickle.load(f)
        # with open('D:/Project/Credit-Card-Fraud-Detection/notebooks/models/dtc.pkl', 'rb') as f:
        #     dtc = pickle.load(f)
        with open('D:/Project/Credit-Card-Fraud-Detection/notebooks/models/rfc.pkl', 'rb') as f:
            rfc = pickle.load(f)

        # y_pred = svc.predict(X)
        # y_pred = lr.predict(X)
        # y_pred = dtc.predict(X)
        y_pred = rfc.predict(X)

        pdf["predicted"] = y_pred
        pdf["correct"] = (y_pred == y_true).astype(int)

        accuracy = (pdf["correct"].sum() / len(pdf)) if len(pdf) > 0 else None

        elapsed = time.time() - start_time

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler("D:/Project/Credit-Card-Fraud-Detection/notebooks/logs/rfc_log.txt"),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger(__name__)

        num_rows = len(pdf)
        logger.info(f"Batch {batch_id} | Quantity: {num_rows} | Accuracy: {accuracy:.4f} | Time: {elapsed:.4f} seconds")

query = value_df.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(process_batch) \
    .start() \
    .awaitTermination()

spark.catalog.clearCache()
spark.stop()