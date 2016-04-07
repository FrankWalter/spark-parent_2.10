
/**
  * Created by liuzh on 2016/3/12.
  */
package org.apache.spark.graphx.OSR

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext} case class Coordinate(x: Double, y: Double) extends Serializable case class Vertex(id: Long, coordinate: Coordinate, category: Int) extends Serializable object Lord extends Serializable{ def main(args: Array[String]): Unit = {
    // Connect to the Spark cluster
    val sparkConf = new SparkConf().setAppName("BrutalForce").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val points: RDD[Vertex] =
      sc.textFile("graphx/data/syntheticpoints.txt")
        .map(line => line.split(","))
        .map( parts =>
          Vertex(parts(0).toLong, Coordinate(parts(1).toDouble, parts(2).toDouble), parts(3).toInt))

    val (newPartition, mbrListWithIndex) =
      STRPartitioner(expectedParNum = 4, sampleRate = 0.3, points)
    val Dpartition = newPartition.map(

    )
    val x = 0
//    rePartitionVertexes(points, 3)
//    def rePartitionVertexes(vertexes: RDD[Vertex], numForEachDimension: Int): Unit = {
//
//    }
  }
}
