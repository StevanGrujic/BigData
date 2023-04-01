FROM bde2020/spark-python-template:3.1.2-hadoop3.2
	  
COPY main.py /app/

ENV SPARK_MASTER spark://spark-master:7077

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/main.py

ENV SPARK_APPLICATION_ARGS "-43.24 -43.26 -22.875 -22.9 2 5 15:43:20 17:44:20"