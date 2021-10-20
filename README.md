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
sbt clean compile assembly
```
+ A Jar will be created in target/scala-3.0.2 named LogProcessing_MapReduce-assembly-0.1.jar
+ scp the jar from the local machine to the Hortonworks HDP VM
```
scp -P 2222 .\LogProcessing_MapReduce-assembly-0.1.jar root@{ip address}:/home/hdfs/logprocessing/
```
+ scp the log file whose analysis is to be computed to the same path in the Hortonworks HDP VM.
+ An example log file is in resources folder named logdata.log

#### Perform the following steps on your VM
+ Go to the directory where you transferred the jar and the input text
+ Create an HDFS directory {hadoop_storage}
```
hdfs dfs -mkdir {hadoop_storage}
```
+ Copy the file from VM's local storage to the HDFS file
```
hdfs dfs -copyFromLocal {input_file} {hadoop_storage}/
```
+ Execute the Jar. Enter 1,2,3,4 based on the map-reduce task that you want to execute
```
hadoop jar LogProcessing_MapReduce-assembly-0.1.jar 4 {hadoop_storage/logdata.log} {hadoop_storage/output_directory}
```
+ Check the output by executing the following command
```
hdfs dfs -cat {hadoop_directory/output_directory}/part-r-00000
```

### Test Cases


### Output
