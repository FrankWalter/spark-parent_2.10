
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

    val e = edges.collect()
    val graph = Graph(points, edges)
      .mapVertices(
        (id, attr) => {
          val category = attr.asInstanceOf[Int]
          if (id == sourceId) {
            VertexMS(id, category, PathRecord[Int](id, 0))
          }
          else VertexMS(id, category, null)
        })

    var sssp = graph
    for (i <- 1 to categoryNum) {
      sssp = sssp.pregel(PathRecord[Int]())(
        (id, attr, message) => {
          if (message == null) {
            attr
          } else if (attr.pathRecord == null ||
            attr.pathRecord.distance > message.distance) {
            attr.copy(pathRecord = message)
          } else {
            attr
          }
        },
        triplet => {
          if (triplet.srcAttr.pathRecord == null) {
            Iterator.empty
          } else if (triplet.dstAttr.pathRecord == null ||
            triplet.srcAttr.pathRecord.distance + triplet.attr <
              triplet.dstAttr.pathRecord.distance) {
            Iterator((triplet.dstId,
              PathRecord[Int](triplet.srcAttr.pathRecord,
                triplet.dstId,
                triplet.srcAttr.pathRecord.distance + triplet.attr
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
      val tmp = sssp.vertices.collect()
      sssp = sssp.mapVertices((id, attr) => {
        if (attr.category != i) {
          attr.copy(pathRecord = null)
        } else {
          attr
        }
      })
    }
    val result = sssp.vertices
      .filter(vertex => {
      vertex._2.category == categoryNum
    })
    val r = result.reduce((a, b) => {
      if (a._2.pathRecord.distance < b._2.pathRecord.distance) a
      else b
    })
    println(r._2.pathRecord.path.toString)
  }
}
