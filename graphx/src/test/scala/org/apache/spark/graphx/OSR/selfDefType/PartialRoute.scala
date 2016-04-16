
/**
  * Created by liuzh on 2016/4/16.
  */
package org.apache.spark.graphx.OSR.selfDefType

import scala.collection.mutable.ArrayBuffer

object PartialRoute{
  def apply(): PartialRoute = {
    null
  }
  def apply(vertexList: ArrayBuffer[Vertex]): PartialRoute = {
    val result = new PartialRoute(vertexList)
    result
  }
  def apply(vertex: Vertex, oldPartialRoute: PartialRoute): PartialRoute = {
    val result = new PartialRoute(vertex +: oldPartialRoute.vertexList)
    result
  }
}

class PartialRoute(vertexListPara: ArrayBuffer[Vertex]) extends Serializable {
  val vertexList: ArrayBuffer[Vertex] = vertexListPara
  val routeLength: Double = {
    val data = this.vertexList.toArray
    if (data.length == 0 || data.length == 1) {
      0.0
    }
    else {
      var tmp: Double = 0.0
      for( i <- 1 until data.length) {
        tmp += data(i).distanceWithOtherVertex(data(i - 1))
      }
      tmp
    }
  }
}
