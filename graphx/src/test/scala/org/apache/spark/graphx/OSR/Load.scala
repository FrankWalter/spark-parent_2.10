
/**
  * Created by liuzh on 2016/4/16.
  */
package org.apache.spark.graphx.OSR

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer


object Load {
  def main(args: Array[String]): Unit = {
    // Connect to the Spark cluster and access data
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
    // Code for Lord Algorithm
    val Tc: Double = greedyDistance
    val Tv: Double = greedyDistance
    val S: RDD[ListBuffer[Vertex]] =
      vertexes.filter(vertex => vertex.category == categoryNum)
      .filter(vertex =>
      vertex.distanceWithOtherVertex(startPoint) <= Tv
    ).map(vertex => {
      new ListBuffer[Vertex]() += vertex
    })
    for(j <- 1 until categoryNum) {
      val i = categoryNum - j
      vertexes.filter(vertex => vertex.category == i)
        .filter(vertex => vertex.distanceWithOtherVertex(startPoint) <= Tv)
      val Siter = S.collect().iterator
      while (Siter.hasNext) {
        val R = Siter.next()
      }
    }
  }
}
