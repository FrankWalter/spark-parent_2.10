
/**
  * Created by liuzh on 2016/3/12.
  */
package org.apache.spark.graphx.OSR
import org.apache.spark.graphx.OSR.selfDefType._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer



object RLord extends Serializable {
  def main(args: Array[String]): Unit = {
//
    // Connect to the Spark cluster
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

    // Code for RLord
    val (newPartition, mbrListWithIndex, rt) =
      STRPartitioner(expectedParNum = 4, sampleRate = 0.3, vertexes)
    val broadCastRtree = sc.broadcast(rt)

    val Tc: Double = greedyDistance
    var Tv: Double = greedyDistance
    var S: RDD[PartialRoute] =
      newPartition.mapPartitionsWithIndex((index, iter) =>
        if (broadCastRtree.value.coorPartitionRangeQuery(startPoint.coordinate, index, Tv)) iter
        else {
          null
        }
      ).filter(item => item._2.category == categoryNum)
        .map(item =>
          PartialRoute(new ArrayBuffer[Vertex]() += item._2)
        )

    for (j <- 1 until categoryNum) {
      val i = categoryNum - j
      val SArray = S.collect()
      // println("preCount" + vertexes.collect().length)
      val qSet = newPartition.mapPartitionsWithIndex((index, iter) =>
        if (broadCastRtree.value.coorPartitionRangeQuery(startPoint.coordinate, index, Tv)) {
          iter.toArray.map(item => item._2).toIterator
        }
        else {
          Iterator.empty
        }
      ).filter(vertex => vertex.category == i)
        .filter(vertex => vertex.distanceWithOtherVertex(startPoint) <= Tv)
      // println("afterCount" + qSet.collect().length)
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
    println(resultRoute.vertexList.toString)
    println(resultRoute.routeLength)
  }

  def MBRForQ2(startPoint: Coordinate, SArray: Array[PartialRoute], Tc: Double): MBR = {
    var result: MBR = null
    val iter = SArray.iterator
    while (iter.hasNext) {
      val p = startPoint
      val partialRoute = iter.next()
      val q = partialRoute.vertexList.head.coordinate
      val a = (Tc - partialRoute.routeLength) / 2
      val c = p.distanceWithOtherCoordinate(q) / 2
      val center = Coordinate((p.x + q.x) / 2, (p.y +q.y) / 2)
      if ( a > c) {
        val b = math.sqrt(a * a - c * c)
        if (p.x == q.x) {
          if (result == null) result = MBR(Coordinate(-b, -a), Coordinate(b, a))
          else result = result.extendMBRWithCoordinate(Coordinate(-b, -a))
            .extendMBRWithCoordinate(Coordinate(b, a))
        }
        else if (p.y == q.y) {
          if (result == null) result = MBR(Coordinate(-a, -b), Coordinate(a, b))
          else result = result.extendMBRWithCoordinate(Coordinate(-a, -b))
            .extendMBRWithCoordinate(Coordinate(a, b))
        }
        else {
          val slope = (p.y - q.y) / (p.x - q.x)
          val sintheta = math.sqrt(slope * slope / (1 + slope * slope))
          val costheta = sintheta / slope
          val x1 = -math.sqrt(1 / (b * b + a * a * slope * slope)) * slope * a * a
          val y1 = b * b * math.sqrt(1 / (b * b + a * a * slope * slope))
          val newy = (x1 - center.x) * sintheta + (y1 - center.y) * costheta + center.y
          val newyy = (-x1 - center.x) * sintheta + (-y1 - center.y) * costheta + center.y

          val slope2 = -1 / slope
          val sintheta2 = math.sqrt(slope2 * slope2 / (1 + slope2 * slope2))
          val costheta2 = sintheta2 / slope2
          val x2 = -math.sqrt(1 / (b * b + a * a * slope2 * slope2)) * slope2 * a * a
          val y2 = b * b * math.sqrt(1 / (b * b + a * a * slope2 * slope2))
          val newx = (x2 - center.x) * costheta - (y2 - center.y) * sintheta + center.x
          val newxx = (-x2 - center.x) * costheta - (-y2 - center.y) * sintheta + center.x

          result = result.extendMBRWithCoordinate(Coordinate(newx, newy))
            .extendMBRWithCoordinate(Coordinate(newx, newyy))
            .extendMBRWithCoordinate(Coordinate(newxx, newy))
            .extendMBRWithCoordinate(Coordinate(newxx, newyy))
        }
      }
    }
    result
  }
}
