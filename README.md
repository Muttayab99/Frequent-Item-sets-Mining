**Frequent Itemset Mining Algorithms**

**Overview**

This project implements three popular association rule mining algorithms: Apriori, PCY (Park-Chen-Yu), and Page Rank algorithm. Additionally, it sets up a streaming pipeline for real-time analysis using Apache Kafka, along with database integration using MongoDB.


**Dependencies**

Python

Apache Kafka

MongoDB

Kafka-Python library


**Introduction**

Apriori Algorithm

The Apriori algorithm is a classical approach to association rule mining. It works by iteratively generating candidate itemsets and pruning those that do not meet a minimum support threshold.


PCY Algorithm

The PCY (Park-Chen-Yu) algorithm is an optimization of the Apriori algorithm that reduces the number of candidate itemsets by using a hash table to count itemset frequencies.


Page Rank Algorithm

The PageRank algorithm is utilized for finding frequent items in an itemset stored in a JSON file. It models the itemset as a graph and determines each item's importance based on its frequency of occurrence and connections to other items.


**Where Our Approach Varies**

Real-Time Streaming Analysis

Adaptation to Streaming Environment

Integration with Apache Kafka and MongoDB

Flexible and Scalable Architecture

Emphasis on Performance Optimization

Seamless Integration

Innovation and Customization


**Why Is Our Method Superior?**

Real-Time Insights

Integrated Easily

Originality and Tailoring

Project Structure


The project directory structure includes:

producer.py

consumer1.py

consumer2.py

consumer3.py

Pre_Processing.py

Bashscript.py


**File Descriptions**

producer.py: Streams preprocessed data in real-time using Apache Kafka.

consumer1.py: Implements the Apriori algorithm in one consumer.

consumer2.py: Implements the PCY algorithm in one consumer.

consumer3.py: Implements a custom analysis in one consumer.

Pre_Processing.py: Implements preprocessing and creates a new JSON file.

Bashscript.py: Used for running Kafka.


**How to Use**
Ensure all dependencies are installed and configured on your system.
Set up Apache Kafka and MongoDB as per the provided instructions.
Run the setup.sh script to initialize Kafka components.
Run the preprocessing.py script to preprocess the dataset.
Start the producer.py script to stream preprocessed data in real-time.
Run the consumer scripts (consumer1.py, consumer2.py, consumer3.py) to execute association rule mining algorithms.
Optionally, modify the database_integration.py script to connect to MongoDB and store the results.


**Execution Instructions**

Install Dependencies

Set Up Apache Kafka and MongoDB

Initialize Kafka Components

Preprocess the Dataset

Start the Producer

Run the Consumer Scripts


**Contributors**

Muhammad Muttayab - [Email: i221949@nu.edu.pk]

M Abubakar Nadeem - [Email: i222003@nu.edu.pk]


**References**

Agrawal, R., & Srikant, R. (1994). Fast algorithms for mining association rules.

Park, J. S., Chen, M. S., & Yu, P. S. (1995). An effective hash-based algorithm for mining association rules.

Han, J., Pei, J., & Yin, Y. (2000). Mining frequent patterns without candidate generation.

Géron, A. (2017). Hands-On Machine Learning with Scikit-Learn and TensorFlow.

Pedregosa, F., Varoquaux, G., Gramfort, A., Michel, V., Thirion, B., Grisel, O., ... & Duchesnay, É. (2011). Scikit-learn: Machine learning in Python.
