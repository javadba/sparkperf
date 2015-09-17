/*
 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.blazedb.sparkperf

import com.blazedb.sparkperf.util.YamlConfiguration
import org.apache.spark._
import org.apache.spark.sql.SQLContext

import scala.reflect.runtime.universe._

abstract class AbstractRDDPerfTest[RddK, RddV](cmdlineArgs: Array[String])
                                              (implicit rddK: TypeTag[RddK], rddV: TypeTag[RddV]) extends java.io.Serializable {

  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  def readTestConfig(fname: String): Product

  @throws(classOf[Exception])
  def setUp(args: Array[String]) {
    val testName = "SparkBenchmark" // TODO: specify Core or SQL
    val sconf = new SparkConf()
        .setAppName(testName)
    val master = if (args.contains("--master")) {
      args(args.indexOf("--master") + 1)
    } else if (sconf.contains("spark.master")) {
      sconf.get("spark.master")
    } else if (System.getenv().containsKey("MASTER")) {
      System.getenv("MASTER")
    } else {
      "local[*]"
    }
    sconf.setMaster(master)
    val msg = s"*** MASTER is $master ****"
    System.err.println(msg)
    tools.nsc.io.File("/tmp/MASTER.txt").writeAll(msg)

    sc = new SparkContext(sconf)
    sc.setLocalProperty("spark.akka.askTimeout", "180")
    sc.setLocalProperty("spark.driver.maxResultSize", "2GB")
    sqlContext = new SQLContext(sc)
  }

    def toIntList(cval: Option[_], default: Seq[Int]): Seq[Int] = {
      import collection.JavaConverters._
      if (cval.isEmpty) {
        default
      } else cval.get match {

        case ints: java.util.ArrayList[_] => ints.asScala.toSeq.asInstanceOf[Seq[Int]]
        case _ => throw new IllegalArgumentException(s"Unexpected type in toIntList ${cval.get.getClass.getName}")
      }
    }
    def toBoolList(cval: Option[_], default: Seq[Boolean]): Seq[Boolean] = {
      import collection.JavaConverters._
      if (cval.isEmpty) {
        default
      } else cval.get match {

        case bools: java.util.ArrayList[_] => bools.asScala.toSeq.asInstanceOf[Seq[Boolean]]
        case _ => throw new IllegalArgumentException(s"Unexpected type in toIntList ${cval.get.getClass.getName}")
      }
    }
    def toLong(cval: Option[Long], default: Long) = cval.getOrElse(default)

  def readConfig(ymlFile: String) = {
    new YamlConfiguration(ymlFile)
  }

  def getArg(name: String, default: String) = cmdlineArgs.contains(name)

  def getArgPair(name: String, default: String) = {
    val ix = cmdlineArgs.indexOf(name)
    if (ix >= 0) {
      cmdlineArgs(ix + 1)
    } else {
      default
    }
  }

  @throws(classOf[Exception])
  def close() {
    sc.stop
  }
}
