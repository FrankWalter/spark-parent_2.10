
/**
  * Created by liuzhe on 2016/3/1.
  * */
package org.apache.spark.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
case class Point(x: Double, y: Double)
object BrutalForce {

  def main(args: Array[String]) {
    // Connect to the Spark cluster
    val sparkConf = new SparkConf().setAppName("BrutalForce").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val users = sc.textFile("graphx/data/users.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) )
    // Load my user data and parse into tuples of user id and attribute list
    val points: RDD[(VertexId, Point)] =
      sc.textFile("graphx/data/syntheticpoints.txt")
        .map(line => line.split(","))
        .map( parts => (parts.head.toLong, Point(parts(1).toDouble, parts(2).toDouble)) )

    val edges: RDD[Edge[Double]] =
      sc.textFile("graphx/data/syntheticedges.txt")
        .map(line => line.split(","))
        .map(data => Edge(data(0).toLong, data(1).toLong, 0.0))
    val graph = Graph(points, edges)
    graph.mapTriplets(
      triplet =>
        Edge(triplet.srcId, triplet.dstId,
          distanceBetweenTwoPoints(
            triplet.srcAttr.asInstanceOf[Point], triplet.dstAttr.asInstanceOf[Point])))
    println(graph.vertices.collect().mkString("\n"))
//    print(x)
//    graph.vertices.map(vertex => vertex._1).map(println)
        // Parse the edge data which is already in userId -> userId format
//    val followerGraph = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")
//
//    // Attach the user attributes
//    val graph = followerGraph.outerJoinVertices(users) {
//      case (uid, deg, Some(attrList)) => attrList
//      // Some users may not have attributes so we set them as empty
//      case (uid, deg, None) => Array.empty[String]
//    }
//
//    // Restrict the graph to users with usernames and names
//    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)
//
//    // Compute the PageRank
//    val pagerankGraph = subgraph.pageRank(0.001)
//
//    // Get the attributes of the top pagerank users
//    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
//      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
//      case (uid, attrList, None) => (0.0, attrList.toList)
//    }
//
//    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
  }

  def distanceBetweenTwoPoints(p1: Point, p2: Point): Double =
    math.sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y))
}
