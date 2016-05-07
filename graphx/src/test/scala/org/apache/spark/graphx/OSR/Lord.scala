
/**
  * Created by liuzh on 2016/4/16.
  */
package org.apache.spark.graphx.OSR

import org.apache.spark.graphx.OSR.selfDefType.{OSRConfig, PartialRoute, Coordinate, Vertex}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object Lord {
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
    val categoryNum = OSRConfig.categoryNum
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
    var Tv: Double = greedyDistance
    var S: RDD[PartialRoute] =
      vertexes.filter(vertex => vertex.category == categoryNum)
        .filter(vertex =>
          vertex.distanceWithOtherVertex(startPoint) <= Tv
        ).map(vertex =>
        PartialRoute(new ArrayBuffer[Vertex]() += vertex)
      )

    for (j <- 1 until categoryNum) {
      val i = categoryNum - j
      val qSet = vertexes.filter(vertex => vertex.category == i)
        .filter(vertex => vertex.distanceWithOtherVertex(startPoint) <= Tv)
      val SArray = S.collect()

      S = qSet.map(q => {
        val SIter = SArray.iterator
        var S2 = PartialRoute()
        while (SIter.hasNext) {
          val R: PartialRoute = SIter.next()
          if (startPoint.distanceWithOtherVertex(q) +
            q.distanceWithOtherVertex(R.vertexList.head) +
            R.routeLength <= Tc) {
            if (S2 == null) S2 = PartialRoute(q, R)
            else if (q.distanceWithOtherVertex(R.vertexList.head) +
              R.routeLength < S2.routeLength) {
              S2 = PartialRoute(q, R)
            }
 //           println("now q is" + q.id + "and S2 is " + S2.vertexList.map(v => v.id).toString)
          }
        }
        S2
      }).filter(pr => pr != null)

      Tv = Tc - S.reduce((pr1, pr2) => {
        if (pr1.routeLength > pr2.routeLength) pr2
        else pr1
      }
      ).routeLength
    }


    val resultRoute: PartialRoute = S.map(pr => PartialRoute(startPoint, pr)).reduce((pr1, pr2) => {
      if (pr1.routeLength > pr2.routeLength) pr2
      else pr1
    }
    )


    println("the vertex list of Lord is " + resultRoute.vertexList.map(v => v.id).toString)
    println("the route length of Lord is " + resultRoute.routeLength)

  }
}
