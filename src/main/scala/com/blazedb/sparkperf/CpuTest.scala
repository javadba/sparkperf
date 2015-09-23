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

import com.blazedb.sparkperf.YsSparkTypes.{InputRDD, Collect, RddKey, RddVal}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import collection.mutable

object CpuMatrix extends TestMatrix {
  override def runMatrix(sc: SparkContext, testDimsProd: Product) = {
    val testDims = testDimsProd.asInstanceOf[CpuTestConfig]
    val passArr = mutable.ArrayBuffer[Boolean]()
    val resArr = mutable.ArrayBuffer[TestResult]()
    val dtf = new SimpleDateFormat("MMdd-hhmmss").format(new Date)
    for (nLoops <- testDims.nLoops;
         nCpus <- testDims.nCpus;
         mem <- testDims.mem;
         nRecs <- testDims.nRecords;
         nPartitions <- testDims.nPartitions
         ) {
      val rawName = "Cpu"
      val tname = s"$dtf/$rawName"
      val name = s"$tname ${nLoops}Loops ${nCpus}cores ${mem}mb ${nRecs}recs ${nPartitions}parts"
      val dir = name.replace(" ", "/")
      val mat = TestMatrixSpec("Cpu", "1.0", GenDataParams(nRecs, nPartitions, Some(0),
        Some(10*1000*1000), Some(1)))
      val dgen = new SingleSkewDataGenerator(sc, mat.genDataParams)
      val rdd = dgen.genData()
      val battery = new CpuBattery(sc, name, dir, rdd, nLoops, nCpus, mem)
      val (pass, tresults) = battery.runBattery()
      val counts = tresults.map{t => t.optCount.getOrElse(-1)}
      trace(rawName,s"Finished test $name with resultCounts=${counts.mkString(",")}")
      passArr += pass
      resArr ++= tresults
    }
    (passArr.forall(identity), resArr)
  }

}

class CpuBattery(sc: SparkContext, testName: String, outputDir: String,
                 inputRdd: InputRDD, nLoops: Int, nCpus: Int, mem: Int)
  extends TestBattery("CoreBattery", s"$outputDir/$testName") {
  assert(inputRdd != null, "Hey null RDD's are not cool")

  override def runBattery() = {
    val bcData = sc.broadcast(nLoops, nCpus, mem, CpuBattery.wasteCpu _)
    val actions = Seq(Collect /*, CollectByKey */)
    val xformRdds = Seq(
      (s"$testName GroupByKey", inputRdd.mapPartitions { case iter =>
        val (lnLoops, lnCpus, lmem, lwasteCpu) = bcData.value
        val startt = System.currentTimeMillis
        val x = lwasteCpu(lnLoops)
        println(s"X=$x duration=${System.currentTimeMillis - startt}")
        iter
      }.groupByKey())
    )
    val gres = for ((name, rdd) <- xformRdds) yield {
      runAggregationTests(name, rdd, actions)
    }

    val countRdds = Seq(
      (s"$testName AggregateByKey", inputRdd.mapPartitions { case iter =>
        val (lnLoops, lnCpus, lmem, lwasteCpu) = bcData.value
        val startt = System.currentTimeMillis
        val x = lwasteCpu(lnLoops)
        println(s"X=$x duration=${System.currentTimeMillis - startt}")

        iter.map { case (k, v) =>
          (k, v)
        }
      }.aggregateByKey(0L)((k, v) => k * v.length, (k, v) => k + v))
    )

    val ares = for ((name, rdd) <- countRdds) yield {
      runCountTests(name, rdd, actions)
    }
    (true, (/*gres ++ */ ares ++ Nil).flatten)
  }


}

case class CpuTestConfig(nLoops: Seq[Int], nCpus: Seq[Int], mem: Seq[Int], nRecords: Seq[Int], nPartitions: Seq[Int])

object CpuBattery {
    def wasteCpu(loops: Int) = {
      var x = 1.0
      for (i <- 1 to loops) {
        x = (x * math.pow(i, 1.2) * i) /
          (math.pow(x, 1.5) * math.sqrt(i) * math.max(math.abs(math.cos(x)), 0.1)
            * math.pow(i, 1.21))
      }
      x
    }
}

class CpuTest(args: Array[String]) extends AbstractRDDTest[RddKey, RddVal](args) {

  val TestsFile = "config/cpuTests.yml"
  override def readTestConfig(ymlFile: String) = {
    val yml = readConfig(ymlFile)
    val conf = CpuTestConfig(
      toIntList(yml("cpu.nLoops.thousands"), Seq(1000)).map(_ * 1000),
      toIntList(yml("cpu.nCpus"), Seq(8)),
      toIntList(yml("cpu.mem.mb"), Seq(4096)),
      toIntList(yml("cpu.nRecords.thousands"), Seq(1000)).map(_ * 1000),
      toIntList(yml("cpu.nPartitions"), Seq(20))
    )
    println(s"CoreTest config is ${conf.toString}")
    conf
  }

  def test(): Boolean = {
    depthTests()
  }

  def depthTests(): Boolean = {
    val testConfig = readTestConfig(getArgPair("test.config.file", TestsFile))
    val (pass, tresults) = CpuMatrix.runMatrix(sc, testConfig)
    pass
  }
}

object CpuTest {

  def main(args: Array[String]) {
    val b = new CpuTest(args)
    b.setUp(args)
    b.test()
  }
}

