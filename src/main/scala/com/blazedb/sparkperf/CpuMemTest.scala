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

import com.blazedb.sparkperf.YsSparkTypes.{RddKey, RddVal}

class CpuMemTest(args: Array[String]) extends AbstractRDDPerfTest[RddKey, RddVal](args) {

  val TestsFile = "config/cpuMemTests.yml"
  override def readTestConfig(ymlFile: String) = {
    val yml = readConfig(ymlFile)
    val conf = CpuMemTestConfig(
      toIntList(yml("cpumem.nLoops.thousands"), Seq(1000)).map(_ * 1000),
      toIntList(yml("cpumem.nCpus"), Seq(8)),
      toIntList(yml("cpumem.mem.mb"), Seq(4096)),
      toIntList(yml("cpumem.nRecords.thousands"), Seq(1000)).map(_ * 1000),
      toIntList(yml("cpumem.nPartitions"), Seq(20))
    )
    println(s"CoreTest config is ${conf.toString}")
    conf
  }

  def test(): Boolean = {
    depthTests()
  }

  def depthTests(): Boolean = {
    val testConfig = readTestConfig(getArgPair("test.config.file", TestsFile))
    val (pass, tresults) = CpuMemMatrix.runMatrix(sc, testConfig)
    pass
  }
}

object CpuMemTest {

  def main(args: Array[String]) {
    val b = new CpuMemTest(args)
    b.setUp(args)
    b.test()
  }
}

