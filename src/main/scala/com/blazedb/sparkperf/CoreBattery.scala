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

  def runAggregationTests(name: String, rdd: AggRDD, actions: Seq[Action]): Seq[TestResult] = {
    val results = for (action <- actions) yield {
      val tname = s"$name $action"
      var tres: TestResult = null
      TimedResult(tname){
        val result = action match {
          case Collect => rdd.collect
          case Count => rdd.count
          case CollectByKey => rdd.collectAsMap
          case CountByKey => rdd.countByKey
          case _ => throw new IllegalArgumentException(s"Unrecognized action $action")
        }
        val size = Some(getSize(result))
        TestResult(tname, tname, size)
      }
    }
    results
  }

  def runCountTests(name: String, rdd: CountRDD, actions: Seq[Action]): Seq[TestResult] = {
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
