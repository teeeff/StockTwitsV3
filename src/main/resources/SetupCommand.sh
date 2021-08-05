sh kafka-topics.sh --create --zookeeper "m01.itversity.com:2181,m02.itversity.com:2181,w01.itversity.com:2181" --replication-factor 2 --partitions 1 --topic tdsktest

sh kafka-console-producer.sh --broker-list "w01.itversity.com:9092,w02.itversity.com:9092,w03.itversity.com:9092" --topic tdsktest

sh kafka-console-consumer.sh --bootstrap-server "w01.itversity.com:9092,w02.itversity.com:9092,w03.itversity.com:9092" --topic tdsktest --from-beginning

java -cp StockTwitsV3-1.0-SNAPSHOT-jar-with-dependencies.jar com.tinufarid.stocktwits_v3.StocktwitsKafkaProducer

java -cp StockTwitsV3-1.0-SNAPSHOT-jar-with-dependencies.jar com.tinufarid.stocktwits_v3.StockTwitsKafkaConsumer

nohup java -cp StockTwitsV3-1.0-SNAPSHOT-jar-with-dependencies.jar com.tinufarid.stocktwits_v3.StocktwitsKafkaProducer >/dev/null 2>&1 &

nohup java -cp StockTwitsV3-1.0-SNAPSHOT-jar-with-dependencies.jar com.tinufarid.stocktwits_v3.StockTwitsKafkaConsumer >/dev/null 2>&1 &
export HADOOP_USER_NAME=hdfs
export SPARK_MAJOR_VERSION=3