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

package au.com.cba.omnia.thermometer
package tools

import com.twitter.scalding._
import scala.util.control.NonFatal
import scalaz._, Scalaz._
import org.apache.hadoop.fs.FileSystem

object Jobs {
  /** Run the specified job, returns an optional error state (None indicates success). */
  def runJob(job: Job): Option[String \/ Throwable] = {
    def start(j : Job, cnt : Int): Option[String \/ Throwable] = try {
      val successful = {
        j.validate
        j.run
      }
      j.clear

      if(successful)
        j.next.flatMap(nextj => start(nextj, cnt + 1))
      else
        Some(s"Job failed to run <${j.name}>".left)
    } catch {
      case NonFatal(e) => Some(e.right)
    } finally {
      FileSystem.closeAll()
    }

    start(job, 0)
  }
}
