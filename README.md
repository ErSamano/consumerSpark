# consumerSpark
Phase 1:
This is the first aproach to DStreams function to create a Kafka Consumer on Spark

Phase 2:
Use the first program to create the consumer using spark streaming to pull all the data generate by Producer

## Content
The idea is to create a consumer to pull data from a Sensor emulating the outcome from IoT (one sensor) and imitate the continuous integration and continuous developer process for the followed pipeline;

1. Data from the IoT generator
  - Python
2. Consume that data through spark.
  - Java
3. The consumer code to Github.
4. Use shippable to make CI/CD

Shippable is going to be the backbone for the pipeline, it means every change on the consumer code is going to be reflect through Github and shippable is going to made the change.

### Type of sensors
* Sensor Name	
* Measurement Timestamp	
* Measurement ID
* Resource ID
* Speed Timestamp
* Channel
* Interval Radiation
* Pressure Response Code
* Pressure Type
* Measurement Timestamp Label
* Channel Type ID
* Data Reception Type
