
/**
  * Created by liuzh on 2016/3/12.
  */
package org.apache.spark.graphx.OSR

import java.io.{ObjectOutputStream, IOException}

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{Partition, SparkConf, SparkContext}

case class Coordinate(x: Double, y: Double) extends Serializable {
  def distanceWithOtherCoordinate(other: Coordinate): Double =
    math.sqrt(
      math.pow(this.x - other.x, 2)
        + math.pow(this.y - other.y, 2)
    )
}

case class Vertex(id: Long, coordinate: Coordinate, category: Int) extends Serializable {
  def distanceWithOtherVertex(other: Vertex): Double =
  this.coordinate.distanceWithOtherCoordinate(other.coordinate)
}

class CartesianPartition(
                          idx: Int,
                          @transient rdd1: RDD[_],
                          @transient rdd2: RDD[_],
                          s1Index: Int,
                          s2Index: Int
                        ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

object Lord extends Serializable {
  def main(args: Array[String]): Unit = {
    def getCartesianPartition(
                               rdd1: RDD[(Coordinate, Vertex)],
                               rdd2: RDD[(Coordinate, Vertex)]): Array[Partition] = {
      // create the cross product split
      val array = new Array[Partition](rdd1.partitions.length * rdd1.partitions.length)
      for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
        val idx = s1.index * rdd2.partitions.length + s2.index
        array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
      }
      array
    }
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
    val cartesianPartition = getCartesianPartition(newPartition, newPartition)
  }
}
