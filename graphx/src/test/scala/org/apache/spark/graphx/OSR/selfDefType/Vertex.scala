
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
  def apply[T](distance: T, id: VertexId, prePathRecord: PathRecord[T]): PathRecord[T] = {
      new PathRecord[T](distance, id, prePathRecord)
  }
  def apply[]
}
class PathRecord[T](distance: T, id: VertexId, prePathRecord: PathRecord[T])
  extends Serializable
