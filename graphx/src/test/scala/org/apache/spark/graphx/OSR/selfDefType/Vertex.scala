
/**
  * Created by liuzh on 2016/4/16.
  */
package org.apache.spark.graphx.OSR.selfDefType

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

case class VertexMS(id: Long, category: Int, preId: Long, distance: Int, active: Boolean)
  extends Serializable
