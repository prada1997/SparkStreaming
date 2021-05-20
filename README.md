# SparkStreaming

Description:

Developed a scala maven program to monitor a folder in HDFS in real time such that any new
file in the folder will be processed. For each RDD in the stream the following task will be perform,

1. Count the word frequency and save the output in HDFS.
2. Filter out the short words i.e <5 characters and save the output in HDFS.
3. Count the co-occurence of words in each RDD where the context is the same line and save the output in the HDFS. 

Requirements to run the program:
Maven
Java7
Scala

Steps to run the program:

1. Create the "Task2_Scala-1.0-SNAPSHOT-jar-with-dependencies" jar file by using maven build using clean and verify goal.

2. Upload the jar in Hue account using website.

3. Create a folder in the files named “input”.

4. Download the jar file in to the Hadoop server by using “hadoop fs -copyToLocal /user/Task2_Scala-1.0-SNAPSHOT-jar-with-dependencies.jar ~/"

5. To run the program execute the command in the Hadoop server "spark-submit --class streaming.NetworkWordCount --master yarn --deploy-mode client Task2_Scala-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:/user/input/ hdfs:/user/output2/sub_taskA_output hdfs:/user/output2/sub_taskB_output hdfs:/user/output2/sub_taskC_output"

6. To generate the output, upload the input files and wait for 10 seconds after uploading each input file in the "input" folder created in the step 3.
