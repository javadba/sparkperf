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

abstract class TestBattery(name: String, outdir: String) {
  def setUp(): Unit = {}

  def runBattery(): (Boolean, Seq[TestResult])

  def tearDown(): Unit = {}

  def runLoopTests(name: String, rdd: GroupedRDD, nLoops: Int, cached: Boolean,
                   actions: Seq[Action]): Seq[TestResult] = {
    val results = for (loop <- 0 until nLoops;
                       action <- actions) yield {
      val tname = s"$name $action Loop$loop"
      var tres: TestResult = null
      TimedResult(tname) {
        if (loop == 0 && cached) {
          rdd.cache
        }
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
    rdd.unpersist(true)
    results
  }

  def runXformTests(name: String, rdd: XformRDD, actions: Seq[Action]): Seq[TestResult] = {
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

  def runAggregationTests(name: String, rdd: GroupedRDD, actions: Seq[Action]): Seq[TestResult] = {
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
}
