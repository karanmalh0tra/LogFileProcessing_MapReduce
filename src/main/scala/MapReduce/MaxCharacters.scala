package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.commons.collections.IteratorUtils
import scala.collection.mutable.ArrayBuffer
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

package object MaxCharacters {
  class NumberOfCharactersMapper extends Mapper[Object, Text, Text, IntWritable]{
    val logger = CreateLogger(classOf[NumberOfCharactersMapper])
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      logger.info("Collecting the length of message from each line of log...")
      val inputLineList = value.toString.split(" ").toList
      word.set(inputLineList(2))
      context.write(word,new IntWritable(inputLineList.last.length))
    }

  }

  class MaxCharacterReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    val logger = CreateLogger(classOf[MaxCharacterReducer])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      logger.info("Computing maximum length of message for log type: ", key)
      //ToDO: replace var and for loop
      var lst = ArrayBuffer[Int]()
      for(i <- values.asScala){
        lst += i.get().toInt
      }
      context.write(key, new IntWritable(lst.max))
    }
  }
}
