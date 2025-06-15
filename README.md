# Real-time machine learning models credit card fraud detection

## Dataset: `https://www.kaggle.com/datasets/kartik2112/fraud-detection/data`

## Tools Required: Apache Kafka, Apache Hadoop, Apache Spark, PySpark
## Python Libraries: pandas, numpy, scikit-learn, matplotlib, pickle, confluent-kafka, pyspark

## How to run

### Models
- If you haven't stored any `.pkl` models yet (check folder `../source/models`), you can run `Modeling.ipynb` to create 4 simple models.

### Apache Kafka
- Using `cmd`
  - Run `C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties` to start Zookeeper server
  - Run `C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties` to start Kafka server
  - Run `C:\kafka\bin\windows\kafka-console-consumer.bat --topic CCT --bootstrap-server localhost:9092 --from-beginning` to start a consumer

### Apache Spark
- You can config file `SparkStream.py` before running Spark to select models and change the name of the ouput log file.
- Using `cmd`
  - Run `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 [Your path to SparkStreaming.py]`

### Start the stream
- Using `VS Code`
  - Run all in `Producer.ipynb` to start producing data
  - Model performances will be generated in folder `../source/logs`
