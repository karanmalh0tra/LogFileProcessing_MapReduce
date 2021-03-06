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

//for each message type you will produce the distribution of log types in a time interval.

package object TimeIntervalDistribution {
  class NumberOfLogsPerSecondMapper extends Mapper[Object, Text, Text, IntWritable]{
    val config = ConfigFactory.load("application")
    val logger = CreateLogger(classOf[NumberOfLogsPerSecondMapper])
    val typeList = config.getList("LOG_LEVELS")

    val word = new Text()
    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      logger.info("Collecting the frequency of logs per second...")
      //fetch timestamp till seconds and ignore the miliseconds in the 0th index of timeStampList
      val timeStampList = value.toString.split('.').toList
      val logType = value.toString.split(" ")(2) //fetch the logtype in the second index after we split on space
      word.set(timeStampList(0)+','+logType)
      context.write(word,one)
    }
  }

  class NumberOfLogsPerSecondReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    val logger = CreateLogger(classOf[NumberOfLogsPerSecondReducer])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      logger.info("Computing count per each second. The current time is: ", key)
      val sum = values.asScala.foldLeft(0)(_ + _.get) //calculate the sum
      context.write(key, new IntWritable(sum))
    }
  }
}
