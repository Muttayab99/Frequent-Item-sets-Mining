#!/bin/bash

# Function to start Zookeeper
start_zookeeper() {
    echo "Starting Zookeeper..."
    /home/hdoop/kafka/bin/zookeeper-server-start.sh /home/hdoop/kafka/config/zookeeper.properties &
    sleep 5
}

# Function to start Kafka broker
start_kafka_broker() {
    echo "Starting Kafka Broker..."
    /home/hdoop/kafka/bin/kafka-server-start.sh /home/hdoop/kafka/config/server.properties &
    sleep 5
}

# Function to start Kafka Connect
start_kafka_connect() {
    echo "Starting Kafka Connect..."
    /home/hdoop/kafka/bin/connect-distributed.sh /home/hdoop/kafka/config/connect-distributed.properties &
    sleep 5
}

# Function to start producer
start_producer() {
    echo "Starting Producer..."
    python /home/hdoop/producer.py &
    sleep 2
}

# Function to start consumers
start_consumers() {
    echo "Starting Consumers..."
    python /home/hdoop/consumer1.py &
    python /home/hdoop/consumer2.py &
    python /home/hdoop/consumer3.py &
    sleep 2
}

# Start Zookeeper
start_zookeeper

# Start Kafka broker
start_kafka_broker

# Start Kafka Connect
start_kafka_connect

# Start Producer
start_producer

# Start Consumers
start_consumers

echo "All components started successfully."