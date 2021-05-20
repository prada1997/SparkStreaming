package streaming

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object NetworkWordCount {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created .split(" ")
    val lines = ssc.textFileStream(args(0))

    //the symbols will be removed by blank in the stream using replaceAll methods.
    val processedWords = lines.map(_.replaceAll("[^a-zA-Z0-9\\s]",""))

    //the stream of words will be split into each words using space as regex.
    val countingWords = processedWords.flatMap(_.trim.split(" +"))

    //the each word will be counted by 1 and all similar words counts will be added.
    //.map method will be used to counting the each words
    //.reduceByKey will be used to calculate total count of similar words
    val wordCounts = countingWords.map(x => (x, 1)).reduceByKey(_ + _)

    //the output of wordsCounts in a word stream will be saved in the new file
    //along with the specified output path and time.
    //if the stream will be blank the output will not be saved.
    wordCounts.foreachRDD({ rdd =>
      rdd.repartition(1)    //only one file will be generated

      //if the stream will be blank the output will not be saved.
      if(!rdd.isEmpty()) {
        //saving the file along with the path and time.
        rdd.saveAsTextFile(args(1) + "_" + DateTimeFormatter.ofPattern("HH-mm-ss").format(LocalDateTime.now) + "/")
//
      }
    })

    //after cleaning the symbols from the word stream
    //the stream is split into words by using .split with space as regex
    //after splitting filtering out the words equal and greater than 5.
    val selectedWords = processedWords.flatMap(_.trim.split(" +").filter(_.length >= 5))

    //the output of selectedWords in a word stream will be saved in the new file
    //along with the specified output path and time.
    //if the stream will be blank the output will not be saved.
    selectedWords.foreachRDD({ rdd =>
      rdd.repartition(1)   //only one file will be generated

      //if the stream will be blank the output will not be saved.
      if(!rdd.isEmpty()) {
        //saving the file along with the path and time.
        rdd.saveAsTextFile(args(2) +"_" + DateTimeFormatter.ofPattern("HH-mm-ss").format(LocalDateTime.now) + "/")

      }
    })

    //after cleaning the symbols from the word stream
    //the stream is split into words by using .split with space as regex
    //the using combination to generate combinations of any 2 words
    //writing the each combination as 1 using .map then reducing and creating the total count of the combinations using reduceByKey
    val wordPair = processedWords.map(x => x.trim.split(" +")).flatMap{_.combinations(2).map{pairs=>(pairs.mkString(","),1)}}.
      reduceByKey(_+_)

    //the output of wordPair in a word stream will be saved in the new file
    //along with the specified output path and time.
    //if the stream will be blank the output will not be saved.
    wordPair.foreachRDD({ rdd =>
      rdd.repartition(1)    //only one file will be generated

      //if the stream will be blank the output will not be saved.
      if(!rdd.isEmpty()) {
        //saving the file along with the path and time.
        rdd.saveAsTextFile(args(3) +"_" + DateTimeFormatter.ofPattern("HH-mm-ss").format(LocalDateTime.now) + "/")

      }
    })

    println(lines)
    println("ALL TASKs FINISHED..!!!")


    ssc.start()
    ssc.awaitTermination()
  }
}
