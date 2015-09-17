package com.blazedb.sparkperf

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import collection.mutable

case class CpuMemTestConfig(nLoops: Seq[Int], nCpus: Seq[Int], mem: Seq[Int], nRecords: Seq[Int], nPartitions: Seq[Int])

object CpuMemMatrix extends TestMatrix {
  override def runMatrix(sc: SparkContext, testDimsProd: Product) = {
    val testDims = testDimsProd.asInstanceOf[CpuMemTestConfig]
    val passArr = mutable.ArrayBuffer[Boolean]()
    val resArr = mutable.ArrayBuffer[TestResult]()
    val dtf = new SimpleDateFormat("MMdd-hhmmss").format(new Date)
    for (nLoops <- testDims.nLoops;
         nCpus <- testDims.nCpus;
         mem <- testDims.mem;
         nRecs <- testDims.nRecords;
         nPartitions <- testDims.nPartitions
         ) {
      val rawName = "CpuMem"
      val tname = s"$dtf/$rawName"
      val name = s"$tname ${nLoops}Loops ${nCpus}cores ${mem}mb ${nRecs}recs ${nPartitions}parts"
      val dir = name.replace(" ", "/")
      val mat = TestMatrixSpec("CpuMem", "1.0", GenDataParams(nRecs, nPartitions, Some(0),
        Some(10*1000*1000), Some(1)))
      val dgen = new SingleSkewDataGenerator(sc, mat.genDataParams)
      val rdd = dgen.genData()
      val battery = new CpuMemBattery(sc, name, dir, rdd, nLoops, nCpus, mem)
      val (pass, tresults) = battery.runBattery()
      val counts = tresults.map{t => t.optCount.getOrElse(-1)}
      trace(rawName,s"Finished test $name with resultCounts=${counts.mkString(",")}")
      passArr += pass
      resArr ++= tresults
    }
    (passArr.forall(identity), resArr)
  }

}

