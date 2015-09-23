/*
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blazedb.sparkperf.util

import java.io.{FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
/**
 * PerfLogger
 *
 */
object PerfLogger  {

  import collection.mutable
  val DATE_FMT = new SimpleDateFormat("<HH:mm:ss>")
  var LogsDir = s"${System.getProperty("user.home")}/sparkperf"
  val lfile= new java.io.File(LogsDir)
  if (!lfile.exists) {
    lfile.mkdirs
  }
  private val logsMap = new mutable.HashMap[String,PrintWriter]()
  def trace(name: String, msg: String, stdout: Boolean = false) = {
    val path = s"$LogsDir/${name.replace(" ","/")}.log"
    val dir = path.substring(0,path.lastIndexOf("/"))
    val f = new java.io.File(dir)
    if (!f.exists) {
      f.mkdirs
    }
    val log = logsMap.getOrElseUpdate(path, new PrintWriter(new FileWriter(path)))
    val omsg = s"${DATE_FMT.format(new Date)} $msg"
    log.println(omsg)
    log.flush()
    if (stdout) {
      println(omsg)
    }
  }

}