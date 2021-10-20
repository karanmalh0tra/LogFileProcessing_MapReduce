# LogFileProcessing
An Apache Hadoop program for experimenting with parallel distributed processing of large dataset of logs using the Map/Reduce Model.

---
Name: Karan Malhotra
---

### Installations
+ Install [Simple Build Toolkit (SBT)](https://www.scala-sbt.org/1.x/docs/index.html)
+ Ensure you can create, compile and run Java and Scala programs.

### Development Environment
+ Windows 10
+ Java 11.0.11
+ Scala 3.0.2
+ SBT Script Version 1.5.5
+ Other dependencies exist in build.sbt and assembly.sbt
+ IntelliJ IDEA Ultimate
+ VMware Workstation Pro
+ Hortonworks HDP Sandbox

### Steps to Run the Application
#### Perform the below steps on your local machine
+ Clone the repository
+ Navigate to the project directory using a terminal
+ Execute the below commands
```
sbt clean compile test
```
```
sbt clean compile assembly
```
+ A Jar will be created in target/scala-3.0.2 named LogProcessing_MapReduce-assembly-0.1.jar
+ scp the jar from the local machine to the Hortonworks HDP VM
```
scp -P 2222 .\LogProcessing_MapReduce-assembly-0.1.jar root@{ip address}:/home/hdfs/logprocessing/
```
+ scp the log file whose analysis is to be computed to the same path in the Hortonworks HDP VM.
+ An example log file is in resources folder named bigdatalog.log

#### Perform the following steps on your VM
+ Go to the directory where you transferred the jar and the input text
+ Create an HDFS directory. Assume the name is hadoop_storage
```
hdfs dfs -mkdir hadoop_storage
```
+ Copy the file from VM's local storage to the HDFS file
```
hdfs dfs -copyFromLocal bigdatalog.log hadoop_storage/
```
+ Execute the Jar. Enter 1,2,3,4 based on the map-reduce task that you want to execute.
+ Example for Tasks 1,3 and 4.
```
hadoop jar LogProcessing_MapReduce-assembly-0.1.jar 4 hadoop_storage/bigdatalog.log hadoop_storage/output4
```
+ Since Task 2 is a combination of 2 Map-Reduce tasks, there is an intermediate directory we need to mention. Thus, to execute the Task 2, use the following commands
```
hadoop jar LogProcessing_MapReduce-assembly-0.1.jar 2 hadoop_storage/bigdatalog.log hadoop_storage/intermediateoutput hadoop_storage/output2
```
+ Check the output by executing the following command
```
hdfs dfs -cat hadoop_storage/output2/part-r-00000
```

### Test Cases
1. `testCheckConfig` tests if the configs are loaded or whether they're null.
2. `testCountOfLogTypes` tests whether all the different log types are present in the config.
3. `testReducerCountTask1` tests whether the number of reducers for Task 1 are > 0.
4. Similar tests are done for `testReducerCountTask2`, `testReducerCountTask3` and `testReducerCountTask4`.
5. `testLogData` tests whether the log file present in the resources folder is empty or not.
6. `testFirstLineOfLog` ensures whether the log format is compatible with the log format this project expects.

### Overview
This Project covers 4 map-reduce jobs.
- Job 1 calculates the number of log messages occured per given time interval. Set to 1s in the project.
- Job 2 counts the number of ERROR log messages in a given time interval and sorts them in a descending order based on the count of those messages.
- Job 3 calculates the occurence of each type of log types in the log messages and outputs it to a file.
- Job 4 displays the maximum log message length for each log type present in the log file.

All the outputs are stored as Comma-Separated Values(CSV).

### Output
The sample outputs from the logs dataset map-reduce implementation would look like:

Output of Job 1:

Output of Job 2:

Output of Job 3:

Output of Job 4:
