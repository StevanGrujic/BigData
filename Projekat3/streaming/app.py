from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import StringIndexerModel, VectorAssembler, MinMaxScalerModel, StandardScalerModel
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.classification import NaiveBayesModel
from pyspark.sql.functions import concat, date_format, lit, to_timestamp, hour, when, col, from_json
from pyspark.sql.types import *

import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
#from influxdb import InfluxDBClient
from datetime import datetime

      
dbhost = os.getenv('INFLUXDB_HOST')
dbport = os.getenv('INFLUXDB_PORT')
dbuser = os.getenv('INFLUXDB_USERNAME')
dbpassword = os.getenv('INFLUXDB_PASSWORD')
dbname = os.getenv('INFLUXDB_DATABASE')
MODEL_LOCATION = os.getenv('MODEL_LOCATION')
SCALER_LOCATION = os.getenv('SCALER_LOCATION')
INDEXER_LOCATION = os.getenv('INDEXER_LOCATION')
KAFKA_URL = os.getenv('KAFKA_URL')
KAFKA_TOPIC = 'riobuses'


class InfluxDBWriter:
    def __init__(self):
        self._org = 'riobusesdb'
        self._token = '2c83186a-caab-425a-9594-9d4c00544939'
        self.client = InfluxDBClient(
            url = "http://influxdb:8086", token=self._token, org = self._org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    def process(self, row):
        self.write_api.write(bucket='riobusesdb',
                             record=self._row_to_line_protocol(row))

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def _row_to_line_protocol(self, row):
        print(row)
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        return Point.measurement(KAFKA_TOPIC).tag("measure", KAFKA_TOPIC) \
                    .field("busID", float(row['busID_num'])) \
                    .field("busLine", float(row['busLine_num'])) \
                    .field("latitude", float(row['latitude'])) \
                    .field("longitude", float(row['longitude'])) \
                    .field("datetime_num", float(row['datetime_num'])) \
                    .field("speed", float(row['speed'])) \
                    .field("prediction_speed", float(row['prediction'])) \
                    .time(timestamp, write_precision='ms')
    

if __name__ == '__main__': 
    
    appName = "Projekat3Stream"
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    model = LinearRegressionModel.load(MODEL_LOCATION)
    scaler = StandardScalerModel.load(SCALER_LOCATION)
    indexer = StringIndexerModel.load(INDEXER_LOCATION)
    
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    schema = StructType(
        [
            StructField("date", StringType()),
            StructField("time", StringType()),
            StructField("busID", StringType()),
            StructField("busLine", StringType()),
            StructField("latitude", StringType()),
            StructField("longitude", StringType()),
            StructField("speed", StringType()),
        ]
    )

    parsed_values = df.select(
        "timestamp", from_json(col("value").cast("string"), schema).alias("parsed_values")
    )

    df_org = parsed_values.selectExpr("timestamp", "parsed_values.date AS date", "parsed_values.time AS time",
                                            "parsed_values.busID AS busID",
                                            "parsed_values.busLine AS busLine",
                                            "parsed_values.latitude AS latitude",
                                            "parsed_values.longitude AS longitude",
                                            "parsed_values.speed AS speed")

    df_org = df_org.withColumn("speed", col("speed").cast("double"))
    df_org = df_org.withColumn("latitude", col("latitude").cast("double"))
    df_org = df_org.withColumn("longitude", col("longitude").cast("double"))

    df_org = df_org.withColumn("datetime", concat(col("date"), lit(" "), col("time")))
    df_org = df_org.withColumn("datetime", to_timestamp("datetime", "MM-dd-yyyy HH:mm:ss"))
    df_org = df_org.drop("date")
    df_org = df_org.drop("time")


    df_org = df_org.withColumn("datetime", date_format("datetime", "MM-dd-yyyy HH:mm:ss"))

    df_org = df_org.filter(df_org.speed <= 120)

    df_org = df_org.filter(df_org.latitude >= -28)
    df_org = df_org.filter(df_org.latitude <= -18)

    df_org = df_org.filter(df_org.longitude >= -48)
    df_org = df_org.filter(df_org.longitude <= -38)

    indexed = indexer.transform(df_org)

    indexed = indexed.drop('busID')
    indexed = indexed.drop('busLine')
    indexed = indexed.drop('datetime')

    columns = ["latitude", "longitude", "datetime_num", "busID_num", "busLine_num"]  # ovde treba i speed ako cemo klasifikaciju

    va = VectorAssembler().setInputCols(columns).setOutputCol('features').setHandleInvalid("skip").transform(indexed)

    scaled = scaler.transform(va)

    predictions = model.transform(scaled)

    predictions.printSchema()

    query = predictions.writeStream \
        .foreach(InfluxDBWriter()) \
        .start()
    
    query.awaitTermination()