
/**
  * Created by liuzh on 2016/5/2.
  */

package org.apache.spark.graphx.OSR

import org.apache.spark.graphx.OSR.selfDefType.VertexMS
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalacheck.Prop.False

object OSRInMS {
  def main(args: Array[String]) {
    // Connect to the Spark cluster
    val sparkConf = new SparkConf().setAppName("BrutalForce").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    // Load my user data and parse into tuples of user id and attribute list
    val sourceId: VertexId = 0
    val points: RDD[(VertexId, VertexMS)] =
      sc.textFile("graphx/data/syntheticpoints2.txt")
        .map(line => line.split(","))
        .map(parts => {
          val (distance, flag) =
            if (parts.head.toLong == sourceId) (0, true)
            else (Int.MaxValue, false)
          (parts.head.toLong, VertexMS(parts.head.toLong,
                                        parts(1).toInt,
                                        -1,
                                        distance,
                                        flag))})


    val edges: RDD[Edge[Int]] =
      sc.textFile("graphx/data/syntheticedges2.txt")
        .map(line => line.split(","))
        .flatMap(data =>
          List(Edge(data(0).toLong, data(1).toLong, data(2).toInt),
            Edge(data(1).toLong, data(0).toLong, data(2).toInt)
          )
        )


    val graph = Graph(points, edges)

    val sssp = graph.pregel(PathRecord(Double.PositiveInfinity, -1, null))(
      (id, dist, newDist) => {
        if (dist.distance < newDist.distance) dist
        else newDist
      },
      triplet => {
        if (triplet.srcAttr.distance + triplet.attr < triplet.dstAttr.distance) {
          Iterator((triplet.dstId,
            PathRecord(triplet.srcAttr.distance + triplet.attr, triplet.srcId, triplet.srcAttr)))
        }
        else {
          Iterator.empty
        }
      },
      (a, b) => {
        if (a.distance < b.distance) a
        else b
      }
    )

    val newsssp = sssp.
      vertices.
      filter((vertex) => {
        vertex._1 % 9 == 8
      })

    val result = newsssp
      .reduce((a, b) => if (a._2.distance > b._2.distance) b else a)
    println("distance: " + result._2.distance)
    println(result._1)
    var tmp = result._2
    while (tmp != null && tmp.preid > 0) {
      println(tmp.preid)
      tmp = tmp.prePathRecord
    }
  }
}
