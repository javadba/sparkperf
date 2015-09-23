/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package com.blazedb.sparkperf

import java.text.SimpleDateFormat
import java.util.Date

import com.blazedb.sparkperf.util.YamlConfiguration
import com.blazedb.sparkperf.YsSparkTypes._
import com.blazedb.sparkperf.util.TimedResult
import org.apache.spark.SparkContext
import collection.mutable

/**
 * How to run this in Intellij :
 *
 * You need 8GB of free memory to run the 100M test.  Comment that test out in CoreBattery if you do not
 * have sufficient memory
 *
 * 1. Create a new run configuration for this class - pointing to the main() method
 *
 * 2. Set JVM Options to
 * -DIGNITE_QUIET=false  -Xloggc:./gc.log  -XX:+PrintGCDetails  -verbose:gc  -XX:+UseParNewGC
 * -XX:+UseConcMarkSweepGC  -XX:+UseTLAB  -XX:NewSize=128m  -XX:MaxNewSize=128m
 * -Xms1024m  -Xmx8192m  -XX:MaxPermSize=512m  -XX:MaxTenuringThreshold=0
 * -XX:SurvivorRatio=1024  -XX:+UseCMSInitiatingOccupancyOnly
 * -XX:CMSInitiatingOccupancyFraction=60
 *
 */

object CoreTestMatrix extends TestMatrix {

  val defaultTestConfig = new CoreTestConfig(
      A(1000), // 1Meg records
      A(20) ,
      A(1),
      0L,
      10000L,
      A(true, false)
  )
  def runMatrix(sc: SparkContext, testDimsProd: Product) = {
    val testDims = testDimsProd.asInstanceOf[CoreTestConfig]
    val passArr = mutable.ArrayBuffer[Boolean]()
    val resArr = mutable.ArrayBuffer[TestResult]()
    val dtf = new SimpleDateFormat("MMdd-hhmmss").format(new Date)
    for (useIgnite <- testDims.useIgnite;
         nRecs <- testDims.nRecords;
         nPartitions <- testDims.nPartitions;
         skew <- testDims.firstPartitionSkew
         ) {

      val rawName = "CoreSmoke"
      val tname = s"$dtf/$rawName"
      val igniteOrNative = if (useIgnite) "ignite" else "native"
      val name = s"$tname ${nRecs}recs ${nPartitions}parts ${skew}skew ${igniteOrNative}"
      val dir = name.replace(" ", "/")
      val mat = TestMatrixSpec("core-smoke", "1.0", GenDataParams(nRecs, nPartitions, Some(testDims.minVal),
        Some(testDims.maxVal), Some(skew)))
      val dgen = new SingleSkewDataGenerator(sc, mat.genDataParams)
      val rdd = dgen.genData()
      val battery = new CoreBattery(sc, name, dir, rdd)
      val (pass, tresults) = battery.runBattery()
      val counts = tresults.map{t => t.optCount.getOrElse(-1)}
      trace(rawName,s"Finished test $name with resultCounts=${counts.mkString(",")}")
      passArr += pass
      resArr ++= tresults
    }
    (passArr.forall(identity), resArr)
  }
}

class CoreBattery(sc: SparkContext, testName: String, outputDir: String,
  inputRdd: InputRDD) extends TestBattery("CoreBattery", s"$outputDir/$testName") {
  assert(inputRdd != null, "Hey null RDD's are not cool")

  override def runBattery() = {
    val xformRdds = Seq(
      (s"$testName BasicMap", inputRdd.map { case (k, v) =>
        (k, s"[${k}]:$v")
      }),
      (s"$testName Filter", inputRdd.filter { case (k, v) =>
        k % 3 != 0
      }),
      (s"$testName MapPartitions", {
        val sideData = Range(0, 100000)
        val bcSideData = sc.broadcast(sideData)
        val irdd = inputRdd.mapPartitions { iter =>
          iter.map { case (k, v) =>
            val localData = bcSideData.value
            (k, 1000.0 + v)
          }
        };
        irdd
      }))
    val actions = Seq(Count, CountByKey)
    val res = for ((name, rdd) <- xformRdds) yield {
      runXformTests(name, rdd, actions)
    }
    val aggRdds = Seq(
      (s"$testName GroupByKey", inputRdd.groupByKey)
    )
    val aggActions = Seq(Count, CountByKey)
    val ares = for ((name, rdd) <- aggRdds) yield {
      runAggregationTests(name, rdd, aggActions)
    }
    val countRdds = Seq(
      (s"$testName AggregateByKey", inputRdd.aggregateByKey(0L)((k, v) => k * v.length, (k, v) => k + v))
    )
    val countActions = Seq(Count, CountByKey)
    val cres = for ((name, rdd) <- countRdds) yield {
      runCountTests(name, rdd, countActions)
    }
    // TODO: determine a pass/fail instead of returning true
    (true, (res ++ ares ++ cres).flatten)
  }


}

case class CoreTestConfig(nRecords: Seq[Int], nPartitions: Seq[Int], firstPartitionSkew: Seq[Int],
                          minVal: Long, maxVal: Long, useIgnite: Seq[Boolean])


class CoreRDDTest(args: Array[String]) extends AbstractRDDTest[RddKey, RddVal](args) {

  def test(): Boolean = {
    depthTests()
  }

  val coreTestsFile: String = "config/coreTests.yml"
  override def readTestConfig(ymlFile: String) = {
    val yml = readConfig(ymlFile)
    val conf = CoreTestConfig(
      toIntList(yml("core.nRecords.thousands"), Seq(1000)).map(_ * 1000),
      toIntList(yml("core.nPartitions"), Seq(20)),
      toIntList(yml("core.skewFactors"), Seq(1)),
      yml("core.minVal").getOrElse(0).asInstanceOf[Int].toLong,
      yml("core.maxVal").getOrElse(100000).asInstanceOf[Int].toLong,
      toBoolList(yml("core.useIgnite"), Seq(true, false))
    )
    println(s"CoreTest config is ${conf.toString}")
    conf
  }

  def depthTests(): Boolean = {
    val testConfig = readTestConfig(getArgPair("CORE_CONFIG_FILE", coreTestsFile))
    val (pass, tresults) = CoreTestMatrix.runMatrix(sc, testConfig)
    pass
  }
}

object CoreRDDTest {
  val CORE_CACHE_NAME = "core"

  def main(args: Array[String]) {
    val b = new CoreRDDTest(args)
    b.setUp(args)
    b.test()

  }
}
