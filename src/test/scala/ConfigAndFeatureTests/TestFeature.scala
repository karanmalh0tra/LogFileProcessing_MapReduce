package ConfigAndFeatureTests

import HelperUtils.ObtainConfigReference
import com.typesafe.config.ConfigFactory
import org.hamcrest.CoreMatchers.hasItems
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.junit.{Assert, Test}
import org.junit.Assert.{assertEquals, assertNotNull, assertThat, assertTrue}

import scala.io.Source
import java.io.File


class TestFeature {

  val config = ConfigFactory.load("application.conf")
  val file = new File(getClass.getClassLoader.getResource("bigdatalog.log").getPath)

  //check if logs exist inside .log file
  @Test
  def testLogData = {
    val fileContentList = Source.fromFile(file).getLines
    assertNotNull(fileContentList)
  }

  // check if .log is of correct format
  @Test
  def testFirstLineOfLog = {
    val firstLine = Source.fromFile(file).getLines.next()
    assertThat(config.getStringList("LOG_LEVELS"),hasItems(firstLine.split(" ")(2)))
  }
}

