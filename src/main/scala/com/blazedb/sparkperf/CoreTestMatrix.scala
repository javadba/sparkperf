package com.blazedb.sparkperf

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import collection.mutable

case class CoreTestConfig(nRecords: Seq[Int], nPartitions: Seq[Int], firstPartitionSkew: Seq[Int],
                          minVal: Long, maxVal: Long, useIgnite: Seq[Boolean])

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
