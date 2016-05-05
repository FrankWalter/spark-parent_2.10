
/**
  * Created by liuzh on 2016/5/2.
  */

package org.apache.spark.graphx.OSR

import java.util.Locale.Category

import org.apache.spark.graphx.OSR.selfDefType.{PathRecord, OSRConfig, VertexMS}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object OSRInMS {
  def main(args: Array[String]) {
    // Connect to the Spark cluster
    val sparkConf = new SparkConf().setAppName("BrutalForce").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    type Category = Int
    // Load my user data and parse into tuples of user id and attribute list
    val sourceId: VertexId = 0
    val categoryNum = OSRConfig.categoryNum
    val points: RDD[(VertexId, Category)] =
      sc.textFile("graphx/data/syntheticpoints2.txt")
        .map(line => line.split(","))
        .map(parts => {
          (parts.head.toLong, parts(1).toInt)
        })

    val edges: RDD[Edge[Int]] =
      sc.textFile("graphx/data/syntheticedges2.txt")
        .map(line => line.split(","))
        .flatMap(data =>
          List(Edge(data(0).toLong, data(1).toLong, data(2).toInt),
            Edge(data(1).toLong, data(0).toLong, data(2).toInt)
          )
        )

    // val p = points.collect()
    val graph = Graph(points, edges)
      .mapVertices(
        (id, attr) => {
          val category = attr.asInstanceOf[Int]
          if (id == sourceId) {
            VertexMS(id, category, PathRecord[Int](0, id, null))
          }
          else VertexMS(id, category, null)
        })

    var sssp = graph
    for (i <- 1 until categoryNum) {
      sssp = sssp.pregel(PathRecord[Int](Int.MaxValue, -1, null))(
        (id, attr, message) => {
          if (attr.pathRecord == null || attr.pathRecord.distance > message.distance) {
            attr.copy(pathRecord = message)
          }
          else  attr
        },
        triplet => {
          if (triplet.srcAttr.pathRecord == null) {
            Iterator.empty
          } else if (triplet.dstAttr.pathRecord == null) {
            Iterator((triplet.dstId,
              PathRecord[Int](triplet.srcAttr.pathRecord.distance + triplet.attr,
                triplet.dstId,
                distance + triplet.attr)))
          }
          if (triplet.srcAttr.pathRecord.distance + triplet.attr < triplet.dstAttr.pathRecord.distance) {
            Iterator((triplet.dstId,
              MessageInMS(triplet.srcId, triplet.srcAttr.distance + triplet.attr)))
          }
        },
        (a, b) => {
          if (a.distance < b.distance) a
          else b
        }
      )
      sssp = sssp.mapVertices((id, attr) => {
        if (attr.category != i) {}
        VertexMS(attr.id, attr.category, attr.preId, Int.MaxValue, active = false)
      })
    }
    //
    //    val newsssp = sssp.
    //      vertices.
    //      filter((vertex) => {
    //        vertex._1 % 9 == 8
    //      })
    //
    //    val result = newsssp
    //      .reduce((a, b) => if (a._2.distance > b._2.distance) b else a)
    //    println("distance: " + result._2.distance)
    //    println(result._1)
    //    var tmp = result._2
    //    while (tmp != null && tmp.preid > 0) {
    //      println(tmp.preid)
    //      tmp = tmp.prePathRecord
    //    }
    println("hello")
  }
}
