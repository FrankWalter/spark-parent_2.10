
/**
  * Created by liuzhe on 2016/3/1.
  * */
package org.apache.spark.graphx.OSR

import org.apache.spark.graphx.OSR.selfDefType.{OSRConfig, Coordinate, PathRecord}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BrutalForce {

  def main(args: Array[String]) {
    // Connect to the Spark cluster
    val sparkConf = new SparkConf().setAppName("BrutalForce").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val categoryNum = OSRConfig.categoryNum

    val points: RDD[(VertexId, Coordinate)] =
      sc.textFile("graphx/data/syntheticpoints.txt")
        .map(line => line.split(","))
        .map( parts => (parts.head.toLong, Coordinate(parts(1).toDouble, parts(2).toDouble)) )

    val edges: RDD[Edge[Double]] =
      sc.textFile("graphx/data/syntheticedges.txt")
        .map(line => line.split(","))
        .map(data => Edge(data(0).toLong, data(1).toLong, 0.0))
    val sourceId: VertexId = 0
    val graph = Graph(points, edges)
      .mapTriplets(
      triplet =>
            triplet.srcAttr.asInstanceOf[Coordinate]
              .distanceWithOtherCoordinate(triplet.dstAttr.asInstanceOf[Coordinate])
            )
        .mapVertices((id, _) => if(id == sourceId) PathRecord[Double](id, 0)
              else null )

    val sssp = graph.pregel(PathRecord[Double]())(
      (id, attr, message) => {
        if (message == null) {
          attr
        } else if (attr == null ||
          attr.distance > message.distance) {
          message
        } else {
          attr
        }
      },
      triplet => {
        if (triplet.srcAttr == null) {
          Iterator.empty
        } else if (triplet.dstAttr == null ||
          triplet.srcAttr.distance + triplet.attr <
            triplet.dstAttr.distance) {
          Iterator((triplet.dstId,
            PathRecord[Double](triplet.srcAttr,
              triplet.dstId,
              triplet.srcAttr.distance + triplet.attr
            )))
        } else {
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
        vertex._1 % (categoryNum + 1) == categoryNum
      })

    val result = newsssp
      .reduce((a,b) => if(a._2.distance > b._2.distance) b else a)
    println("distance: " + result._2.distance)
    println(result._2.path)
  }

  def distanceBetweenTwoPoints(p1: Coordinate, p2: Coordinate): Double =
    math.sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y))
}
