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

import java.io.Serializable

import com.blazedb.sparkperf.YsSparkTypes._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object YsSparkTypes {
  type RddKey = Long
  type RddVal = String
  type RddTuple = (RddKey, RddVal)
  type InputRDD = RDD[RddTuple]
  type InputDataFrame = DataFrame
  type XformRDD = RDD[RddTuple]
  type GroupedRDD = RDD[(RddKey, Iterable[RddVal])]
  type CountRDD = RDD[(RddKey, Long)]

  sealed abstract class Action(name: String)

  case object Collect extends Action("collect")

  case object Count extends Action("count")

  case object CountByKey extends Action("countByKey")

  case object CollectByKey extends Action("collectByKey")

  case object CollectAsMap extends Action("collectAsMap")

  case object SqlCollect extends Action("collect")

  case object SqlCollectByKey extends Action("collectByKey")

  case object SqlCollectAsList extends Action("collectAsList")

}


case class GenDataParams(nRecords: Int, nPartitions: Int, optMin: Option[Long] = None, optMax: Option[Long] = None,
                         optSkewFactor: Option[Int] = Some(1), optRowlen: Option[Int] = None) extends Serializable

abstract class DataGenerator(dataParams: GenDataParams, optRddIn: Option[InputRDD]) extends Serializable {
  var optData = optRddIn

  def getData() = optRddIn.getOrElse(genData(Some(dataParams.optMin.get), Some(dataParams.optMax.get)))

  def getData(min: Long, max: Long) = optRddIn.getOrElse(genData(Some(min), Some(max)))

  def genData(optMin: Option[Long] = None, optMax: Option[Long] = None): InputRDD = optRddIn.get
}


class ProvidedDataGenerator(sc: SparkContext, dataParams: GenDataParams)(rddIn: InputRDD) extends DataGenerator(dataParams, Some(rddIn))

class FileDataGenerator(sc: SparkContext, dataParams: GenDataParams, path: String, delim: Char) extends ProvidedDataGenerator(sc, dataParams)(
{
  val rdd = sc.textFile(path, dataParams.nPartitions)
  rdd.map { l =>
    val toks = l.split(delim)
    (toks(0).toLong, toks(1))
  }
}) {}

class SingleSkewDataGenerator(sc: SparkContext, dataParams: GenDataParams)
  extends DataGenerator(dataParams, None) {
  override def genData(optMinIn: Option[Long] = dataParams.optMin, optMaxIn: Option[Long] = dataParams.optMax) = {

    val dataToBc = new java.io.Serializable {
      val optMin = optMinIn
      val optMax = optMaxIn
      val params = dataParams
      val nPartitions = dataParams.nPartitions
      val nRecords = dataParams.nRecords
      val nrecsPerSlice = Math.ceil(dataParams.nRecords / (dataParams.nPartitions * 2.0)).toInt
      val width = (optMax.get - optMin.get) / dataParams.nPartitions
      val firstNrec = dataParams.nRecords - (dataParams.nPartitions - 1) * nrecsPerSlice
      val nrecsAndBounds = (0 until nPartitions)
        .foldLeft(new mutable.ArrayBuffer[(Int, (Long, Long))]()) { case (m, rx) =>
        m += Tuple2(if (rx == 0) firstNrec else nrecsPerSlice, (1L * rx * width, (rx + 1L) * width))
      }
      val words = DataGeneratorUtils.readWords
      println(s"len(words) is ${words.length}")
      val nWords = words.size
      val optRowlen = dataParams.optRowlen
    }
    val bcData = sc.broadcast(dataToBc)
    val rddSeq = sc.parallelize((0 until dataParams.nPartitions).toSeq, dataParams.nPartitions)
    val rdd = rddSeq.mapPartitionsWithIndex { case (partx, iter) =>
      def nextLong(rng: java.util.Random, n: Long) = {
        var bits = 1L
        var out = 1L
        do {
          bits = (rng.nextLong() << 1) >>> 1
          out = bits % n
        } while (bits - out + (n - 1) < 0L)
        out
      }
      def longs(nrecs: Int, min: Long, max: Long) = {
        val rnd = new java.util.Random
        (0 until nrecs).foldLeft(Vector[Long]()) { case (v, ix) =>
          v :+ nextLong(rnd, max - min) + min
        }
      }
      val rnd = new java.util.Random
      val locData = bcData.value
      val iterout = iter.map { ix =>
        val (nrecs, (lbound, ubound)) = locData.nrecsAndBounds(partx)
        assert(nrecs > 0, s"nrecs is not positive $nrecs")
        assert(ubound > lbound, s"ubound $ubound < lbound $lbound")
        val mlongs = longs(nrecs, lbound, ubound)
        val out = (0 until nrecs).foldLeft(mutable.ArrayBuffer[RddTuple]()) { case (m, n) =>
          val windex = rnd.nextInt(locData.nWords)
          val word = locData.words(windex)
          if (word == null) throw new IllegalStateException("could not find word from dictionary")
          if (word.length == 0) throw new IllegalStateException("Dictionary word length is 0")
          val repeatWord = if (locData.optRowlen.isDefined) {
            word * (locData.optRowlen.get / word.length)
          } else {
            word
          }
          m += Tuple2(mlongs(n), repeatWord)
        }
        out
      }
      iterout.flatten
    }
    rdd
  }
}


object DataGeneratorUtils {
  def readWords() = {
    val text = scala.io.Source.fromFile("src/main/resources/aliceInWonderland.txt").mkString("")
    println(s"alice textlen = ${text.size}")
    val rmtext = text.map { c => c match {
      case _ if """~$!@#`%^&*()-_=+[{}}'";;,.<>?""".contains(c) => ' '
      case '\n' => ' '
      case _ => c
    }
    }.toString
    val words = rmtext.split(" ").filter(_.length > 0)
    words
  }

}