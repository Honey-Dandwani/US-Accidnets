# US-Accidnets
Performed exploratory data analysis on more than 3million rows of US accidents data with 53 fields to identify accident hotspots in Hive and Hadoop.

Final Project for Engineering Big Data Systems - INFO 7250 (Spring 2022)

Data Source: https://www.kaggle.com/sobhanmoosavi/us-accidents
Pre-requisites:

Have JDK 1.8, AWS CLI, Git, Hadoop, Hive & Pig installed in your local file system
MapReduce Question Set:

! To run, execute ./commands.sh
+ To run on AWS EMR, execute ./aws.sh

    Number of Accidents Per Month
    Number of Accidents vs Hour of Day
    Number of Accidents Per Day of the Week
    Side of the Road Percentage
    Number of Accidents Per State
    Top 10 Accident Prone States (Filtering Techniques Top n Filtering Pattern)
    State - Cities -> Inverted Index
    Average, Min and Max Temperature per Severity
    Count of Accidents Per State Per Year (SecondarySorted with 5 Partitions)
    Divide file into partitions divided by state (Inner Join w/ Partitioner)
    Proximity to Traffic Object (Percentage / per all traffic)

HIVE Question Set: (./hive)

! To execute hive file >> hive -f ./hive/queries.hql

    Number of Accidents per timezone
    Number of Accidents per Severity per State (Partitioning)
    Top 10 - Weather Condition vs Number of. Accident

Pig Question Set: (./pig)

! To run, in Pig Grunt shell >> run ./pig/q1.pig 
! To run, in Pig Grunt shell >> run ./pig/q2.pig

    Top 10 Weather Conditions during Accidents in Massachusetts (MA)
    Cities in Massachusetts where accidents happen during snowy days (Replicated Inner Join)

