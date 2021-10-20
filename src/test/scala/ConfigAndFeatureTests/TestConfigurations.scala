package ConfigAndFeatureTests

import HelperUtils.ObtainConfigReference
import com.typesafe.config.ConfigFactory
import org.hamcrest.CoreMatchers.{hasItems, is}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.junit.{Assert, Test}
import org.junit.Assert.{assertEquals, assertNotNull, assertThat, assertTrue}

import java.io.File
import scala.io.Source


class TestConfigurations {

  val config = ConfigFactory.load("application.conf")
  val file = new File(getClass.getClassLoader.getResource("bigdatalog.log").getPath)

  //check if configurations exist
  @Test
  def testCheckConfig = {
    assertNotNull(config)
  }

  // check if all 8 log types are in the configuration
  @Test
  def testCountOfLogTypes = {
    val firstLine = Source.fromFile(file).getLines.next()
    assertThat(config.getStringList("LOG_LEVELS").size(),is(8))
  }

  // check if number of reducers are more than 0
  @Test
  def testReducerCountTask1 = {
    assertTrue(config.getInt("NO_OF_REDUCERS_TASK1") > 0)
  }

  @Test
  def testReducerCountTask2 = {
    assertTrue(config.getInt("NO_OF_REDUCERS_TASK2") > 0)
  }

  @Test
  def testReducerCountTask3 = {
    assertTrue(config.getInt("NO_OF_REDUCERS_TASK3") > 0)
  }

  @Test
  def testReducerCountTask4 = {
    assertTrue(config.getInt("NO_OF_REDUCERS_TASK4") > 0)
  }
}

