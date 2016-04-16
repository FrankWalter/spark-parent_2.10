
/**
  * Created by liuzh on 2016/3/12.
  */
package org.apache.spark.graphx.OSR

import java.io.{ObjectOutputStream, IOException}

import org.apache.spark.graphx.OSR.selfDefType.{Coordinate, Vertex}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer



object RLord extends Serializable {
  def main(args: Array[String]): Unit = {
//
    // Connect to the Spark cluster
    val sparkConf = new SparkConf().setAppName("BrutalForce").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val vertexes: RDD[Vertex] =
      sc.textFile("graphx/data/syntheticpoints.txt")
        .map(line => line.split(","))
        .map(parts =>
          Vertex(parts(0).toLong, Coordinate(parts(1).toDouble, parts(2).toDouble), parts(3).toInt))

    // Code for compute greedy distance
    val categoryNum = 8
    val startPoint: Vertex =
      vertexes.filter((vertex) => vertex.category == 0).collect()(0)
    val greedyDistance: Double = {
      val greedySeq: Array[Vertex] = new Array[Vertex](categoryNum + 1)
      greedySeq(0) = startPoint
      for (i <- 1 to categoryNum) {
        greedySeq(i) =
          vertexes.filter((vertex) => vertex.category == i).reduce((v1, v2) => {
            val d1 = v1.distanceWithOtherVertex(greedySeq(i - 1))
            val d2 = v2.distanceWithOtherVertex(greedySeq(i - 1))
            if (d1 < d2) v1 else v2
          })
      }
      var result: Double = 0.0
      for (i <- 1 to categoryNum) {
        result += greedySeq(i).distanceWithOtherVertex(greedySeq(i - 1))
      }
      result
    }

    // Code for nested loop computation
    val (newPartition, mbrListWithIndex, rt) =
      STRPartitioner(expectedParNum = 4, sampleRate = 0.3, vertexes)
    val broadCastRtree = sc.broadcast(rt)

    val S: ArrayBuffer[ArrayBuffer[Vertex]] = new ArrayBuffer[ArrayBuffer[Vertex]]()
    val Tc: Double = greedyDistance
    val Tv: Double = greedyDistance


    // class CartesianPartition(
    //                          idx: Int,
    //                          @transient rdd1: RDD[_],
    //                          @transient rdd2: RDD[_],
    //                          s1Index: Int,
    //                          s2Index: Int
    //                        ) extends Partition {
    //  var s1 = rdd1.partitions(s1Index)
    //  var s2 = rdd2.partitions(s2Index)
    //  override val index: Int = idx
    //
    //  @throws(classOf[IOException])
    //  private def writeObject(oos: ObjectOutputStream): Unit = selfDefType.tryOrIOException {
    //    // Update the reference to parent split at the time of task serialization
    //    s1 = rdd1.partitions(s1Index)
    //    s2 = rdd2.partitions(s2Index)
    //    oos.defaultWriteObject()
    //  }
    //}
//    def getCartesianPartitionArray(
    //                                    rdd1: RDD[(Coordinate, Vertex)],
    //                                    rdd2: RDD[(Coordinate, Vertex)],
    //                                    mbrListWithIndex: List[(MBR, Int)],
    //                                    benchMark: Double): Array[Partition] = {
    //      // create the cross product split
    //      val array = new Array[Partition](rdd1.partitions.length * rdd1.partitions.length)
    //      for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
    //        val idx = s1.index * rdd2.partitions.length + s2.index
    //        val mbr1 = mbrListWithIndex(s1.index)._1
    //        val mbr2 = mbrListWithIndex(s2.index)._1
    //        array(idx) = {
    //          if (mbr1.minDistanceWithOtherMBR(mbr2) <= benchMark) {
    //            new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    //          }
    //          else null
    //        }
    //      }
    //      array
    //    }
    //    val cartesianPartitionRDD =
    //    for( part <- cartesianPartitionArray) {
    //      part.asInstanceOf[CartesianPartition].hashCode()
    //    }
    //    val tmp = newPartition.mapPartitionsWithIndex((index, part) => {
    //      while(part.hasNext) {
    //        val data = part.next()
    //        println("index " + index + data.toString())
    //      }
    //      part
    //    }
    //    ).collect()
    //    val x = 0
    //    val cartesianPartitionArray = getCartesianPartitionArray(newPartition,
    //      newPartition,
    //      mbrListWithIndex,
    //      greedyDistance)
  }
}
