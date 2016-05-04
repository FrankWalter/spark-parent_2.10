
/**
  * Created by liuzhe on 2016/3/1.
  * */
package org.apache.spark.graphx.OSR

import org.apache.spark.graphx.OSR.selfDefType.{Coordinate, Vertex}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
case class PathRecord(distance: Double, preid: VertexId, prePathRecord: PathRecord)
object BrutalForce {

  def main(args: Array[String]) {
    // Connect to the Spark cluster
    val sparkConf = new SparkConf().setAppName("BrutalForce").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val users = sc.textFile("graphx/data/users.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) )
    // Load my user data and parse into tuples of user id and attribute list
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
        .mapVertices((id, _) => if(id == sourceId) PathRecord(0.0, id, null)
              else PathRecord(Double.PositiveInfinity, id, null))

    val vs = graph.vertices.collect()
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
      .reduce((a,b) => if(a._2.distance > b._2.distance) b else a)
    println("distance: " + result._2.distance)
    println(result._1)
    var tmp = result._2
    while(tmp != null && tmp.preid > 0) {
      println(tmp.preid)
      tmp = tmp.prePathRecord
    }
  }

  def distanceBetweenTwoPoints(p1: Coordinate, p2: Coordinate): Double =
    math.sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y))
}
