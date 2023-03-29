docker cp $HOME/data/. namenode:/data

docker exec -it namenode hdfs dfs -test -d /data
if [ $? -eq 1 ]
then
  echo "[INFO]: Creating /data folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /data
fi

docker exec -it namenode hdfs dfs -test -e /data/RioBuses.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding csv file in the /data folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /data/RioBuses.csv /data/RioBuses.csv
fi
