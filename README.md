# kafka-spark-streaming

This is a personal project to create a streaming solution using kafka, pyspark and docker.

The solution architecture is:

![Solution Architecture](<Solution Architecture.jpg>)

# To execute it

1. build your virtual env using requirement.txt
2. docker-compose up -d
3. Enable host networking
4. run `py producer.py [kafka topic name]`
5. copy pyspark_test.py to /opt/spark-app within your spark container
6. run `docker exec -it pyspark-local spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /opt/spark-app/pyspark_test.py`
