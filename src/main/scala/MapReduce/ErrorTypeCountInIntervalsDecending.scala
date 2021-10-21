package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import scala.io.Source
import java.lang
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.collection.JavaConverters.*
import java.util.StringTokenizer

//This will produce the count of Error type in a specific time interval. 1s here.

package object ErrorTypeCountInIntervalsDecending {
  class ErrorTypeCountInIntervalsDecendingMapper extends Mapper[Object, Text, Text, IntWritable] {
    val config = ConfigFactory.load("application")
    val logger = CreateLogger(classOf[ErrorTypeCountInIntervalsDecendingMapper])
    val typeList = config.getList("LOG_LEVELS")

    val word = new Text()
    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      logger.info("Collecting the frequency of logs per second...")
      //splits and removes the millisecond. Stores the timestamp only upto seconds in the 0th index.
      //Hence easy to split the distribution accross seconds.
      val timeStampList = value.toString.split('.').toList
      val logType = value.toString.split(" ")(2) //stores the logtype
      //since we only require the log type to be ERROR
      if (logType.equals("ERROR")) {
        word.set(timeStampList(0) + ',' + logType)
        context.write(word, one)
      }
    }
  }

    class ErrorTypeCountInIntervalsDecendingReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
      val logger = CreateLogger(classOf[ErrorTypeCountInIntervalsDecendingReducer])

      override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
        logger.info("Computing count per each second. The current time is: ", key)
        val sum = values.asScala.foldLeft(0)(_ + _.get)
        context.write(key, new IntWritable(sum))
      }
    }
  }

