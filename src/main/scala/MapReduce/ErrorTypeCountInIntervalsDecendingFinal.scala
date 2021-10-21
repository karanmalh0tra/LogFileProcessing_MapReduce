package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}

import scala.io.Source
import java.lang
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConverters.*
import java.util.StringTokenizer

//This will produce the count of Error type in a specific time interval. 1s here.

package object ErrorTypeCountInIntervalsDecendingFinal {
  class ErrorTypeCountInIntervalsDecendingMapperFinal extends Mapper[Object, Text, IntWritable, Text] {
    val logger = CreateLogger(classOf[ErrorTypeCountInIntervalsDecendingMapperFinal])

    val word = new Text()
    val one = new IntWritable(1)

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      logger.info("Collecting the frequency of logs per second...")
      //Splitting the output from the previous reducer on tab
      val keyValList = value.toString.split("\\t").toList
      //switching the key and value so the reducer sorts it based on the count.
      //since the default behaviour is ascending order, we multiply by -1 here with an idea to multiply by -1 again in the reducer
      context.write(new IntWritable(-1*keyValList(1).toInt), new Text(keyValList(0)))
    }
  }

  class ErrorTypeCountInIntervalsDecendingReducerFinal extends Reducer[IntWritable, Text, Text, IntWritable] {
    val logger = CreateLogger(classOf[ErrorTypeCountInIntervalsDecendingReducerFinal])

    override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      //the keys we receive are multiplied by -1 to get the actual count. (x*-1*-1 = x)
      val value = key.toString.toInt * -1
      //fetch only the unique elements since aggregation was done based on count and not the node type/timestamp
      val uniqueList = values.asScala.toList.distinct
      uniqueList.foreach(i => context.write(new Text(i), new IntWritable(value)))
    }
  }
}


