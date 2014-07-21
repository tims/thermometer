package au.com.cba.omnia.thermometer.example

import com.twitter.scalding.TypedPsv
import scalaz.effect.IO
import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.core._
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids.recordsByDirectory
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntegrationSpec extends ThermometerSpec { def is = s2"""

Demonstration of testing output against files
=============================================

  Verify output using files            $facts
  Verify output using files2           $facts2

"""

  case class Car(model: String, year: Int) {
    def toPSV = s"${model}|${year}"
  }

  val data = List(
    Car("Canyonero", 1999),
    Car("Batmobile", 1966))

  val psvReader = ThermometerRecordReader[Car]((conf, path) => IO {
    new Context(conf).lines(path).map(line => {
      val parts = line.split('|')
      Car(parts(0), parts(1).toInt)
    })
  })

  def pipeline =
    ThermometerSource[Car](data)
      .map(c => c.model -> c.year)
      .write(TypedPsv[(String, Int)]("output/cars/1"))
      .write(TypedPsv[(String, Int)]("output/cars/2"))

  // All files under the env resource will get copied into the test hadoop user dir.
  val environment = path(getClass.getResource("env").toString)
  def facts = withEnvironment(environment)({
    pipeline
      .withFacts(
        path("output") ==> recordsByDirectory(psvReader, psvReader, path("expected")))
  })
  
  def pipeline2 = {
    ThermometerSource[Car](data)
      .map(c => c.model -> c.year)
      .write(TypedPsv[(String, Int)]("output2/cars/1"))
      .write(TypedPsv[(String, Int)]("output2/cars/2"))
  }

  val environment2 = path(getClass.getResource("env2").toString)
  def facts2 = withEnvironment(environment2)({
    pipeline2
      .withFacts(
        path("output2") ==> recordsByDirectory(psvReader, psvReader, path("expected2")))
  })
}

