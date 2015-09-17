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

import com.blazedb.sparkperf.YsSparkTypes._
import com.blazedb.sparkperf.util.TimedResult
import org.apache.spark.SparkContext


class CpuMemBattery(sc: SparkContext, testName: String, outputDir: String,
                    inputRdd: InputRDD, nLoops: Int, nCpus: Int, mem: Int)
  extends TestBattery("CoreBattery", s"$outputDir/$testName") {
  assert(inputRdd != null, "Hey null RDD's are not cool")

  override def runBattery() = {
    val bcData = sc.broadcast(nLoops, nCpus, mem)
    val xformRdds = Seq(
      (s"$testName GroupByKey", inputRdd.mapPartitions { case iter =>
        val (lnLoops, lnCpus, lmem) = bcData.value
        var x = 1.0
        val startt = System.currentTimeMillis
        for (i <- 1 to lnLoops) {
          x = (x * math.pow(i, 1.2) * i) /
            (math.pow(x, 1.5) * math.sqrt(i) * math.max(math.abs(math.cos(x)), 0.1)
              * math.pow(i, 1.21))
        }
        println(s"X=$x duration=${System.currentTimeMillis - startt}")
        iter.map { case (k, v) =>
          (k, v)
        }
      }))
    val actions = Seq(Count, CountByKey)
    val res = for ((name, rdd) <- xformRdds) yield {
      runXformTests(name, rdd, actions)
    }
    (true, (res ++ Nil).flatten)
  }

  def getSize(x: Any) = {
    import collection.mutable
    x match {
      case x: Number => x.intValue
      case arr: Array[_] => arr.length
      case m: mutable.Map[_, _] => m.size
      case m: Map[_, _] => m.size
      case _ => throw new IllegalArgumentException(s"What is our type?? ${x.getClass.getName}")
    }
  }

  def runXformTests(name: String, rdd: InputRDD, actions: Seq[Action]): Seq[TestResult] = {
    val results = for (action <- actions) yield {
      val tname = s"$name $action"
      var tres: TestResult = null
      TimedResult(tname) {
        val result = action match {
          case Collect => rdd.collect
          case Count => rdd.count
          case CollectByKey => rdd.collectAsMap
          case CountByKey => rdd.countByKey
          case _ => throw new IllegalArgumentException(s"Unrecognized action $action")
        }
        TestResult(tname, tname, Some(getSize(result)))
      }
    }
    results
  }

}
