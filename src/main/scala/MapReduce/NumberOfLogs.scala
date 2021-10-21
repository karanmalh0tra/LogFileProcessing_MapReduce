package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}

import scala.io.Source

//for each message type you will produce the number of the generated log messages.

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

package object CountLogTypes {
  class NumberOfLogsMapper extends Mapper[Object, Text, Text, IntWritable]{
    val config = ConfigFactory.load("application")
    val logger = CreateLogger(classOf[NumberOfLogsMapper])
    val typeList = config.getList("LOG_LEVELS")

    val word = new Text()
    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      logger.info("Collecting the frequency of each log types...")
      val inputLineList = value.toString.split(" ")
      word.set(inputLineList(2)) //index 2 gives the log type when a line of log is split by space
      context.write(word,one)
    }

  }

  class NumberOfLogsReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    val logger = CreateLogger(classOf[NumberOfLogsReducer])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      logger.info("Computing count for log type: ", key)
      val sum = values.asScala.foldLeft(0)(_ + _.get) //adds the total for each log type
      context.write(key, new IntWritable(sum))
    }
  }
}