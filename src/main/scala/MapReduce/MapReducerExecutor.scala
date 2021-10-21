package MapReduce

import HelperUtils.CreateLogger
import MapReduce.CountLogTypes.{NumberOfLogsMapper, NumberOfLogsReducer}
import MapReduce.ErrorTypeCountInIntervalsDecending.{ErrorTypeCountInIntervalsDecendingMapper, ErrorTypeCountInIntervalsDecendingReducer}
import MapReduce.ErrorTypeCountInIntervalsDecendingFinal.{ErrorTypeCountInIntervalsDecendingMapperFinal, ErrorTypeCountInIntervalsDecendingReducerFinal}
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

    //First Task
    if(args(0).contentEquals("1")){
      /*execute first map reduce job
      counts number of log messages per second outputs it in a file.*/
      val configuration = new Configuration
      //for , separated values
      configuration.set("mapred.textoutputformat.separator",",")
      val job = Job.getInstance(configuration,"count logs per second")
      job.setJarByClass(this.getClass)
      //Set Mapper and Reducers
      job.setMapperClass(classOf[NumberOfLogsPerSecondMapper])
      job.setCombinerClass(classOf[NumberOfLogsPerSecondReducer])
      job.setReducerClass(classOf[NumberOfLogsPerSecondReducer])
      job.setNumReduceTasks(config.getInt("NO_OF_REDUCERS_TASK1"))
      job.setOutputKeyClass(classOf[Text]);
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(1)))
      FileOutputFormat.setOutputPath(job, new Path(args(2)))
      System.exit(if(job.waitForCompletion(true))  0 else 1)
    }
      //Second Task
    else if(args(0).contentEquals("2") && args.length > 3){
      //Requires two map-reduce jobs for this task
      /*
      counts number of error logs per second outputs it in a file.
      */
      val configuration = new Configuration
      val job1 = Job.getInstance(configuration,"count error logs per second")
      job1.setJarByClass(this.getClass)
      //Set Mapper and Reducers
      job1.setMapperClass(classOf[ErrorTypeCountInIntervalsDecendingMapper])
      job1.setCombinerClass(classOf[ErrorTypeCountInIntervalsDecendingReducer])
      job1.setReducerClass(classOf[ErrorTypeCountInIntervalsDecendingReducer])
      job1.setNumReduceTasks(config.getInt("NO_OF_REDUCERS_TASK2"))
      job1.setOutputKeyClass(classOf[Text]);
      job1.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job1, new Path(args(1)))
      FileOutputFormat.setOutputPath(job1, new Path(args(2)))
      //job2 can only be executed when job1 finishes
      job1.waitForCompletion(true)

      /*
      sort the error logs in seconds interval in a descending order of the errors present in the intervals
      */
      val configuration2 = new Configuration
      //for , separated values
      configuration2.set("mapred.textoutputformat.separator",",")
      val job2 = Job.getInstance(configuration2,"count error logs per second final")
      job2.setJarByClass(this.getClass)
      //Set Mapper and Reducers
      job2.setMapperClass(classOf[ErrorTypeCountInIntervalsDecendingMapperFinal])
      job2.setReducerClass(classOf[ErrorTypeCountInIntervalsDecendingReducerFinal])
      job2.setNumReduceTasks(config.getInt("NO_OF_REDUCERS_TASK2"))
      job2.setMapOutputKeyClass(classOf[IntWritable]);
      job2.setMapOutputValueClass(classOf[Text]);
      job2.setOutputKeyClass(classOf[Text]);
      job2.setOutputValueClass(classOf[IntWritable]);
      //output of the first map-reduce is the input to the second one
      FileInputFormat.addInputPath(job2, new Path(args(2)))
      FileOutputFormat.setOutputPath(job2, new Path(args(3)))
      System.exit(if(job2.waitForCompletion(true))  0 else 1)
    }
      //Third Task
    else if(args(0).contentEquals("3")){
      /*
      counts each type of log messages and outputs it in a file.
      log types include ALL, INFO, WARN, DEBUG, ERROR, FATAL, TRACE, OFF
      */
      val configuration = new Configuration
      //for , separated values
      configuration.set("mapred.textoutputformat.separator",",")
      val job = Job.getInstance(configuration,"count log types")
      job.setJarByClass(this.getClass)
      //Set Mapper and Reducers
      job.setMapperClass(classOf[NumberOfLogsMapper])
      job.setCombinerClass(classOf[NumberOfLogsReducer])
      job.setReducerClass(classOf[NumberOfLogsReducer])
      job.setNumReduceTasks(config.getInt("NO_OF_REDUCERS_TASK3"))
      job.setOutputKeyClass(classOf[Text]);
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(1)))
      FileOutputFormat.setOutputPath(job, new Path(args(2)))
      System.exit(if(job.waitForCompletion(true))  0 else 1)
    }
      //Fourth Task
    else if(args(0).contentEquals("4")){
      /*
      displays the maximum log message length for each log type
      */
      val configuration = new Configuration
      //for , separated values
      configuration.set("mapred.textoutputformat.separator",",")
      val job = Job.getInstance(configuration,"max length of message types")
      job.setJarByClass(this.getClass)
      //Set Mapper and Reducers
      job.setMapperClass(classOf[NumberOfCharactersMapper])
      job.setCombinerClass(classOf[MaxCharacterReducer])
      job.setReducerClass(classOf[MaxCharacterReducer])
      job.setNumReduceTasks(config.getInt("NO_OF_REDUCERS_TASK4"))
      job.setOutputKeyClass(classOf[Text]);
      job.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job, new Path(args(1)))
      FileOutputFormat.setOutputPath(job, new Path(args(2)))
      System.exit(if(job.waitForCompletion(true))  0 else 1)
    }
  }
}
