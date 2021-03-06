package com.blazedb

import com.blazedb.sparkperf.util.{TimedResult, PerfLogger}
import org.apache.spark.SparkContext

package object sparkperf {

  case class TestResult(testName: String, resultName: String,
                        optCount: Option[Int] = None, optResult: Option[Any] = None, 
                        stdOut: Option[String] = None, stdErr: Option[String] = None) {
    override def toString() = {
      s"$resultName: count=${optCount.getOrElse("Zero")}"
    }
  }

  def trace(msg: String): Unit = trace(getClass.getName, msg)
  def trace(testName: String, msg: String): Unit = PerfLogger.trace(testName, msg, true)

  trait TestMatrix {
    val A = Array

    def runMatrix(sc: SparkContext, testDims: Product): (Boolean, Seq[TestResult])

  }

  case class TestMatrixSpec(name: String, version: String, genDataParams: GenDataParams)


}
