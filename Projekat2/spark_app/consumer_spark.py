import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import *
from pyspark.sql.functions import mean, min, max, col, count, round
from pyspark.sql.functions import concat, to_timestamp, lit
import sys

def Inicijalizacija():

    spark = SparkSession.builder.appName("Projekat2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "locations")
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
    df_org = df_org.filter(df_org.speed <= 120)
    return df_org

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def VratiStatistickeVrednosti(df,long1=None, long2=None, lat1=None, lat2=None):
    if (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2)
        ).groupBy(window(df.timestamp, "10 seconds", "10 seconds")).agg(
            mean(df.speed).alias("mean_speed"),
            min(df.speed).alias("min_speed"),
            max(df.speed).alias("max_speed"),
            count(df.speed).alias("count_speed")
        )

        return df_ret

    else:
        return None

def VratiTopNLokacija(df, N=5, num_decimal_places = 3):
    #Vrsimo zaokruzivanje na odredjeni broj decimala

    df_rounded_locations = df.select(round("latitude", num_decimal_places).alias("latitude_rounded"),
                                      round("longitude", num_decimal_places).alias("longitude_rounded"))

    df_with_window = df_rounded_locations \
                    .groupBy(window(df.timestamp, "10 seconds", "10 seconds"), "latitude_rounded", "longitude_rounded").agg(
                        count("latitude_rounded").alias("freq")
                        #count("*").alias("freq")
                        )

    top_n_locations = df_with_window.orderBy("freq", ascending=False).limit(N)

    return top_n_locations

def writeToCassandra1(writeDF, _):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="statistika", keyspace="spark_keyspace")\
            .save()
        writeDF.show()

def writeToCassandra2(writeDF, _):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="top_locations", keyspace="spark_keyspace")\
            .save()
        writeDF.show()

def izvrsenjeSirinaIDuzina(df):

    long1, long2, lat1, lat2 = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])

    print("Statističke vrednosti za slučaj kada su prosleđene samo širina i dužina")
    df_statistika = VratiStatistickeVrednosti(df, long1=long1, long2=long2, lat1=lat1, lat2=lat2)
    df_statistika_baza = df_statistika.selectExpr("window.start as start",
                                           "window.end as end",
                                           "mean_speed",
                                           "min_speed",
                                           "max_speed",
                                           "count_speed")

    print("Pronalazak top N lokacija")
    df_top_n_locations = VratiTopNLokacija(df)
    df_top_n_locations_baza = df_top_n_locations.selectExpr("window.start as start",
                                           "window.end as end",
                                           "latitude_rounded",
                                           "longitude_rounded",
                                           "freq"
                                           )

    query1 = (df_statistika_baza.writeStream
                    .option("spark.cassandra.connection.host","cassandra:9042")
                    .foreachBatch(writeToCassandra1)
                    .outputMode("update")
                    .start())

    query2 = (df_top_n_locations_baza.writeStream
                    .option("spark.cassandra.connection.host","cassandra:9042")
                    .foreachBatch(writeToCassandra2)
                    .outputMode("complete")
                    .start())

    query1.awaitTermination()
    query2.awaitTermination()

if __name__ == '__main__':

    brojArgumenata = len(sys.argv)

    if brojArgumenata < 2:
        print("Usage: main.py <input folder> ")
        exit(-1)

    if (brojArgumenata != 5):
        print("Lose prosledjeni argumenti")
        exit(-1)

    if brojArgumenata == 5:
        if (isfloat(sys.argv[1])) & (isfloat(sys.argv[2])) & (isfloat(sys.argv[3])) & (isfloat(sys.argv[4])):
            df = Inicijalizacija()
            izvrsenjeSirinaIDuzina(df)
            
        else:
            print("Lose prosledjeni argumenti")
            exit(-1)
    else:
        print("Lose prosledjeni argumenti")
        exit(-1)