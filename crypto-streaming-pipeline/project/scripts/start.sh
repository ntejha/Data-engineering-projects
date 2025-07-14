#!/bin/bash

echo "Starting Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

echo "Starting Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

echo "Starting Cassandra..."
$CASSANDRA_HOME/bin/cassandra -p $CASSANDRA_HOME/cassandra.pid

echo "Waiting 30s for services to stabilize..."
sleep 30

echo "Creating Kafka topic..."
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --if-not-exists \
  --topic crypto-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Starting CoinLore API Producer..."
python3 $HOME/project/producer.py > producer.log 2>&1 &

echo "Starting Spark Streaming..."
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf "spark.ui.port=8080" \
  $HOME/project/spark_streaming.py > spark.log 2>&1 &

echo "Starting Streamlit Dashboard..."
streamlit run $HOME/project/dashboard.py --server.port 8501 > streamlit.log 2>&1 &

echo -e "\n\n=== MONITORING PORTS ==="
echo "Spark Jobs UI:   http://$(hostname -I | awk '{print $1}'):8080"
echo "Streamlit Dashboard: http://$(hostname -I | awk '{print $1}'):8501"
echo "Cassandra CQLSH: cqlsh $(hostname -I | awk '{print $1}') 9042"
echo "Kafka Broker:    $(hostname -I | awk '{print $1}'):9092"

echo -e "\nUse 'tail -f *.log' to monitor logs"
echo "Services running in background. Press Ctrl+C to exit this script (services will continue running)."

while true; do sleep 3600; done