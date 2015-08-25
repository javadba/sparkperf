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
package com.blazedb.sparkperf.util

import com.blazedb.sparkperf.TestResult
import org.slf4j.LoggerFactory
import PerfLogger.trace

class TimedResult extends Serializable {
  val logger = LoggerFactory.getLogger(getClass)
  val formatter = java.text.NumberFormat.getIntegerInstance

  import reflect.runtime.universe._
  def apply[R: TypeTag](name: String)(block: => R): R = {
    trace(name, s"Starting $name ")
    val start = System.currentTimeMillis
    val result = block
    val secs = ((System.currentTimeMillis-start)/1000.0)
    val cmsg = result match {
      case t: TestResult => t.optCount.getOrElse(0)
      case _ => 0
    }
    trace(name, s"Completed $name - duration=$secs seconds count=$cmsg", true)
    result
  }

}

object TimedResult {
  import reflect.runtime.universe._
  def apply[R: TypeTag](name: String)(block: => R): R = new TimedResult()[R](name){block}
}
