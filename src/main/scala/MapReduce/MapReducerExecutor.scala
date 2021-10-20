package MapReduce

import HelperUtils.CreateLogger
import MapReduce.CountLogTypes.{NumberOfLogsMapper, NumberOfLogsReducer}
import MapReduce.MaxCharacters.{MaxCharacterReducer, NumberOfCharactersMapper}
import MapReduce.TimeIntervalDistribution.{NumberOfLogsPerSecondMapper, NumberOfLogsPerSecondReducer}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


object MapReduceExecutor {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("application")

    if(args(0).contentEquals("1")){
      /*execute first map reduce job
      counts number of log messages per second outputs it in a file.*/
      val configuration = new Configuration
      val job = Job.getInstance(configuration,"count logs per second")
      job.setJarByClass(this.getClass)
      job.setMapperClass(classOf[NumberOfLogsPerSecondMapper])
      job.setCombinerClass(classOf[NumberOfLogsPerSecondReducer])
      job.setReducerClass(classOf[NumberOfLogsPerSecondReducer])
      job.setOutputKeyClass(classOf[Text]);
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(1)))
      FileOutputFormat.setOutputPath(job, new Path(args(2)))
      System.exit(if(job.waitForCompletion(true))  0 else 1)
    }
    else if(args(0).contentEquals("2")){
      //execute second map reduce job
    }
    else if(args(0).contentEquals("3")){
      /*execute third map reduce job
      counts each type of log messages and outputs it in a file.
      log types include ALL, INFO, WARN, DEBUG, ERROR, FATAL, TRACE, OFF*/
      val configuration = new Configuration
      val job = Job.getInstance(configuration,"count log types")
      job.setJarByClass(this.getClass)
      job.setMapperClass(classOf[NumberOfLogsMapper])
      job.setCombinerClass(classOf[NumberOfLogsReducer])
      job.setReducerClass(classOf[NumberOfLogsReducer])
      job.setOutputKeyClass(classOf[Text]);
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(1)))
      FileOutputFormat.setOutputPath(job, new Path(args(2)))
      System.exit(if(job.waitForCompletion(true))  0 else 1)
    }
    else if(args(0).contentEquals("4")){
      /*execute fourth map reduce job
      displays the maximum log message length for each log type
      */
      val configuration = new Configuration
      val job = Job.getInstance(configuration,"max length of message types")
      job.setJarByClass(this.getClass)
      job.setMapperClass(classOf[NumberOfCharactersMapper])
      job.setCombinerClass(classOf[MaxCharacterReducer])
      job.setReducerClass(classOf[MaxCharacterReducer])
      job.setOutputKeyClass(classOf[Text]);
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(1)))
      FileOutputFormat.setOutputPath(job, new Path(args(2)))
      System.exit(if(job.waitForCompletion(true))  0 else 1)
    }
  }
}
