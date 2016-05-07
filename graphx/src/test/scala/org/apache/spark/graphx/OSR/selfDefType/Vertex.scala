
/**
  * Created by liuzh on 2016/4/16.
  */
package org.apache.spark.graphx.OSR.selfDefType

import org.apache.spark.graphx._

case class Coordinate(x: Double, y: Double) extends Serializable {
  def distanceWithOtherCoordinate(other: Coordinate): Double =
    math.sqrt(
      math.pow(this.x - other.x, 2)
        + math.pow(this.y - other.y, 2)
    )
}

case class Vertex(id: Long, coordinate: Coordinate, category: Int) extends Serializable {
  def distanceWithOtherVertex(other: Vertex): Double =
    this.coordinate.distanceWithOtherCoordinate(other.coordinate)
}

case class VertexMS(id: Long, category: Int, pathRecord: PathRecord[Int])
  extends Serializable

// case class MessageInMS(srcId: Long, distance: Int)  extends Serializable
object PathRecord {
  def apply[T](): PathRecord[T] = {
    null
  }
  def apply[T](vertexId: VertexId, iniDistance: T): PathRecord[T] = {
    new PathRecord[T](List(vertexId), iniDistance)
  }
  def apply[T](path: List[VertexId], distance: T): PathRecord[T] = {
    new PathRecord[T](path, distance)
  }

  def apply[T](prePathRecord: PathRecord[T],
               newVertexId: VertexId,
               newDistance: T
               ): PathRecord[T] = {
    new PathRecord[T](prePathRecord.path :+ newVertexId, newDistance)
  }
}

class PathRecord[T](pathPara: List[VertexId], distancePara: T) extends Serializable{
  val path = pathPara
  val distance = distancePara
}
