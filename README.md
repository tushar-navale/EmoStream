# EC-Team-57-emostream-concurrent-emoji-broadcast-over-event-driven-architecture

# Commands:
    -> Check Kafka on Port 9092: Once Kafka is running, verify it’s accessible on port 9092:
        netstat -an | grep 9092

    -> main commands
    sudo systemctl start kafka
    sudo systemctl status kafka

    ->start flask api normally 
    python3 emoji_producer.py

    ->start flask api using gunicorn
    gunicorn -w 4 -k gthread --threads 100 -b 0.0.0.0:5000 emoji_producer:app

    -> sometimes the kafka topic is not created , to do it manually:
    /usr/local/kafka/bin/kafka-topics.sh --create --topic emoji_topic --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1

    ->For the intermediate topic emoji_highest_topic:
    /usr/local/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 4 \
  --topic emoji_highest_topic

    ->For the output topic cluster_topic
    /usr/local/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 4 \
  --topic cluster_topic

    ->the cluster_publisher will output to cluster_1 from where subscriber will read
    /usr/local/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 4 \
  --topic cluster_1

    ->the cluster_publisher will output to cluster_2 from where subscriber will read
    /usr/local/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 4 \
  --topic cluster_2

    -> to find if the kafka topic is created:
    /usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

    -> to delete the old_topic
    /usr/local/kafka/bin/kafka-topics.sh --delete --topic emoji_topic --bootstrap-server localhost:9092

    -> this command will start spark as well as allows spark to connect to kafka topics directly and read in real time
    /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 emoji_spark.py

PUB-SUB commands:(all in different terminal)
    till spark all commands are same
    -> python3 main_publisher.py

    -> cd CLUSTERS/
    -> python3 cluster_publisher_1.py
    -> python3 cluster_publisher_2.py
    -> python3 subscriber_1_1.py
    -> python3 subscriber_2_1.py
    -> python3 subscriber_1_2.py
    -> python3 subscriber_2_2.py
    -> python3 sub_c1.py
    -> python3 sub_c2.py
    -> python3 sub_c3.py
    -> python3 sub_c4.py
    -> cd ..

    cd CLIENTS/
    -> python3 c1.py
    -> python3 c2.py
    -> python3 c3.py

    OR

    -> python3 api_spam_script.py
    

