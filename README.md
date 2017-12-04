

# Crime Alert and Analytics System

# Description

Perform analytics and visualize the crime data, find the relations between
the demographic data of an area and the crime data of that area to find the
possible cause of crime and allocate more police forces to those areas

# Architecture

Inline-style: 
![alt text](images/systemArchitecture.png "System Architecure")


# Implementation

1) Stream the events to Kafka Topic ( IBM Bluemix Message Hub)
2) Consume the event using Spark Streaming
3) Perform tansformation using Spark/Hive
4) Connect to Tableau to vizualize the data



