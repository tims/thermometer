//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.thermometer.example

import com.twitter.scalding._

import scalaz.effect.IO

import au.com.cba.omnia.thermometer.core._
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.fact.PathFactoids._

class TypedPsvSpec extends ThermometerSpec { def is = s2"""

Demonstration of ThermometerSpec
================================

  Verify output using explicit expectations                 $expectations
  Verify output using fact api                              $facts

"""

  case class Car(model: String, year: Int) {
    def toPSV = s"${model}|${year}"
  }

  val data = List(
    Car("Canyonero", 1999),
    Car("Batmobile", 1966)
  )

  def pipeline =
    ThermometerSource[Car](data)
      .map(c => c.model -> c.year)
      .write(TypedPsv[(String, Int)]("cars"))

  def expectations =
    pipeline
      .withExpectations(t => {
        t.exists("cars" </> "_SUCCESS") must beTrue
        t.exists("cars" </> "part-00000") must beTrue
        t.lines("cars" </> "part-*") must contain(allOf(data.map(_.toPSV):_*))
      })


  def facts =
    pipeline
      .withFacts(
        "cars" </> "_ERROR"      ==> missing
      , "cars" </> "_SUCCESS"    ==> exists
      , "cars" </> "part-00000"  ==> (exists, count(data.size))
      )
}
