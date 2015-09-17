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

import com.blazedb.sparkperf.util.YamlConfiguration
import YsSparkTypes.{RddKey, RddVal}

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
class CoreRDDPerfTest(args: Array[String]) extends AbstractRDDPerfTest[RddKey, RddVal](args) {

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

object CoreRDDPerfTest {
  val CORE_CACHE_NAME = "core"

  def main(args: Array[String]) {
    val b = new CoreRDDPerfTest(args)
    b.setUp(args)
    b.test()

  }
}
