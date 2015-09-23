package com.blazedb.sparkperf

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import collection.mutable
import YsSparkTypes._

object MemMatrix extends TestMatrix {
  override def runMatrix(sc: SparkContext, testDimsProd: Product) = {
    val testDims = testDimsProd.asInstanceOf[MemTestConfig]
    val passArr = mutable.ArrayBuffer[Boolean]()
    val resArr = mutable.ArrayBuffer[TestResult]()
    val dtf = new SimpleDateFormat(
      "MMdd-hhmmss").format(new Date)
    for (rowlen <- testDims.rowlen;
        // mem <- testDims.mem;
         nRecs <- testDims.nRecords;
         nPartitions <- testDims.nPartitions;
         cached <- testDims.cached;
         nLoops <- testDims.nLoops
    ) {
      val rawName = "Mem"
      val tname = s"$dtf $rawName"
      val name = s"$tname ${rowlen}rowlen ${nRecs}recs ${nPartitions}parts ${nLoops}Loops ${if (cached) "cached" else ""}"
      val dir = name.replace(" ", "/")
      val mat = TestMatrixSpec("Mem", "1.0", GenDataParams(nRecs, nPartitions, Some(0),
        Some(10 * 1000 * 1000), Some(1), Some(rowlen)))
      val dgen = new SingleSkewDataGenerator(sc, mat.genDataParams)
      val rdd = dgen.genData()
      val battery = new MemBattery(sc, name, dir, rdd, rowlen, nLoops, cached)
      val (pass, tresults) = battery.runBattery()
      val counts = tresults.map { t => t.optCount.getOrElse(-1) }
      trace(rawName, s"Finished test $name with resultCounts=${counts.mkString(",")}")
      passArr += pass
      resArr ++= tresults
    }
    (passArr.forall(identity), resArr)
  }
}

class MemBattery(sc: SparkContext, testName: String, outputDir: String,
                 inputRdd: InputRDD, mem: Int, nLoops: Int, cached: Boolean)
  extends TestBattery("CoreBattery", s"$outputDir/$testName") {
  assert(inputRdd != null, "Hey null RDD's are not cool")

  override def runBattery() = {
    val actions = Seq(Collect /*, CollectByKey */)
    val groupedRdds = Seq(

      (s"$testName GroupByKey", inputRdd.mapPartitions { case iter =>
        iter
      }.groupByKey())
    )

    val ares = for ((name, rdd) <- groupedRdds) yield {
      runLoopTests(name, rdd, nLoops, cached, actions)
    }
    (true, (ares ++ Nil).flatten)
  }
}

class MemTest(args: Array[String]) extends AbstractRDDTest[RddKey, RddVal](args) {

  val TestsFile = "config/memTests.yml"

  override def readTestConfig(ymlFile: String) = {
    val yml = readConfig(ymlFile)
    val conf = MemTestConfig(
      toIntList(yml("mem.rowlen"), Seq(1)),
//      toIntList(yml("mem.mb"), Seq(1000)) /*.map(_ * 1024 * 1024) */ ,
      toIntList(yml("mem.nRecords.thousands"), Seq(1000)).map(_ * 1000),
      toIntList(yml("mem.nPartitions"), Seq(20)),
      toBoolList(yml("mem.cached"), Seq(true, false)),
      toIntList(yml("mem.nLoops"), Seq(4))
    )
    println(s"CoreTest config is ${conf.toString}")
    conf
  }

  def test(): Boolean = {
    depthTests()
  }

  def depthTests(): Boolean = {
    val testConfig = readTestConfig(getArgPair("test.config.file", TestsFile))
    val (pass, tresults) = MemMatrix.runMatrix(sc, testConfig)
    pass
  }
}

case class MemTestConfig(rowlen: Seq[Int], nRecords: Seq[Int], nPartitions: Seq[Int], cached: Seq[Boolean], nLoops: Seq[Int])

object MemTest {

  def main(args: Array[String]) {
    val b = new MemTest(args)
    b.setUp(args)
    b.test()
  }
}

