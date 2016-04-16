
/**
  * Created by liuzh on 2016/4/16.
  */
package org.apache.spark.graphx.OSR.selfDefType

import scala.collection.mutable.ArrayBuffer

object PartialRoute{
  def apply(vertexList: ArrayBuffer[Vertex]): PartialRoute = {
    val result = new PartialRoute(vertexList)
    result
  }
}

class PartialRoute(vertexList: ArrayBuffer[Vertex]) {
  val this.vertexList: ArrayBuffer[Vertex] = vertexList
}
