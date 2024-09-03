# Big Data Projects: Rio Buses Dataset Analysis

## Repository Overview

This repository contains three related projects that focus on the analysis and processing of the "Rio Buses" dataset. This dataset includes public transportation data for Rio de Janeiro, collected between October 1, 2010, and October 30, 2010. Each project builds on the previous one, progressively adding more complex data processing and analysis techniques.

### Project 1: Basic Dataset Analysis with PySpark

#### Overview

The first project introduces basic big data processing techniques using PySpark. It allows for the analysis of the Rio Buses dataset based on various geographical, temporal, and data-specific criteria.

#### Dataset Attributes

- Date
- Time
- Bus ID
- Bus Line
- Latitude
- Longitude
- Speed

#### Application Functionality

The application processes the dataset using various command-line arguments to filter and analyze data. Key functionalities include:

- **Data Filtering**: Based on longitude, latitude, date, and time parameters.
- **Data Analysis**: Calculation of average speeds, peak travel times, and mapping bus routes.

#### Key Functions

- **izvrsenjeSve(df)**: Comprehensive analysis using all input parameters.
- **izvrsenjeSirinaIDuzina(df)**: Geographical analysis using longitude and latitude.
- **izvrsenjeDatum(df)**: Date-based analysis.
- **izvrsenjeVreme(df)**: Time-based analysis.
- **izvrsenjeDatumaIVremena(df)**: Combined date and time analysis.

### Project 2: Real-Time Data Processing with Spark and Flink

#### Overview

The second project extends the first by introducing real-time data processing capabilities using Apache Spark and Apache Flink. It demonstrates how to process streaming data from the Rio Buses dataset.

#### Components

1. **Spark Application**: Processes real-time streaming data, filters it based on geographical boundaries, and stores the results in a Cassandra database.
   
   - **Libraries Used**: Apache Spark, Apache Kafka, Apache Cassandra
   - **Command-Line Arguments**: Longitude1, Longitude2, Latitude1, Latitude2
   
2. **Flink Application**: Similar to the Spark application but implemented in Java, with additional functionalities such as statistical calculations and top N location identification.
   
   - **Key Classes**: DataStreamJob, AverageAggregate, TopNLocationsAggregate, CassandraService

### Project 3: Advanced Streaming with Machine Learning and Visualization

#### Overview

The third project builds on the previous work by incorporating machine learning and data visualization into the real-time processing pipeline. The focus is on training a linear regression model and applying it to streaming data.

#### Components

1. **Model Training Application**: Trains a linear regression model on the Rio Buses dataset using PySpark.
   
   - **Libraries Used**: Apache Spark, HDFS
   - **Key Functionalities**: Data preprocessing, model training, and model storage in HDFS.

2. **Streaming Application**: Applies the trained model to real-time data streams, stores the results in InfluxDB, and visualizes them using Grafana.
   
   - **Libraries Used**: Apache Spark Streaming, Apache Kafka, InfluxDB, Grafana

3. **Visualization in Grafana**: Configures Grafana dashboards to provide real-time visual insights into the processed data.

## Setup Instructions

### Prerequisites

- Apache Kafka
- Apache Cassandra
- Apache Spark
- Apache Flink (for Project 2)
- InfluxDB (for Project 3)
- Grafana (for Project 3)
- HDFS (for Project 3)
- Java Development Kit (for Project 2 and 3)

## Conclusion

This repository demonstrates the application of big data processing techniques across three progressive projects. Starting from basic data analysis, it advances to real-time processing and machine learning, providing a comprehensive pipeline for handling large-scale transportation data.
