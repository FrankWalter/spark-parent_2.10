
/**
  * Created by liuzh on 2016/3/26.
  */
package org.apache.spark.graphx.OSR.selfDefType

case class MBR(min: Coordinate, max: Coordinate) extends Serializable {
  val center: Coordinate = Coordinate((min.x + max.x) / 2, (min.y + max.y) / 2)
  val width: Double = max.x - min.x
  val height: Double = max.y - min.y

  def extendMBRWithCoordinate(coor: Coordinate): MBR = {
    MBR(Coordinate(math.min(this.min.x, coor.x), math.min(this.min.y, coor.y)),
      Coordinate(math.max(this.max.x, coor.x), math.max(this.max.y, coor.y)))
  }
  def minDistanceWithCoordinate(coor: Coordinate): Double = {
    if (coor.x <= this.max.x && coor.x >= this.min.x
      && coor.y <= this.max.y && coor.y >= this.min.y) {
      0.0
    } else {
      if (coor.x <= this.min.x) {
        // Left side
        if (coor.y <= this.min.y) {
          // Down side
          coor.distanceWithOtherCoordinate(this.min)
        } else if (coor.y >= this.max.y) {
          // Up side
          coor.distanceWithOtherCoordinate(Coordinate(this.min.x, this.max.y))
        } else {
          // Middle
          this.min.x - coor.x
        }
      } else if (coor.x >= this.max.x){
        // Right side
        if (coor.y <= this.min.y) {
          // Down side
          coor.distanceWithOtherCoordinate(Coordinate(this.max.x, this.min.y))
        } else if (coor.y >= this.max.y) {
          // Up side
          coor.distanceWithOtherCoordinate(this.max)
        } else {
          // Middle
          coor.x - this.max.x
        }
      } else {
        if (coor.y >= this.max.y) {
          coor.y - this.max.y
        } else {
          this.min.y - coor.y
        }
      }
    }

  }
  def minDistanceWithOtherMBR(other: MBR): Double = {
    if (this.isIntersectWithOtherMBR(other)) 0.0
    else if (this.max.x <= other.min.x) {
      // Left side
      if (this.max.y <= other.min.y) {
        // Down side
        this.max.distanceWithOtherCoordinate(other.min)
      }
      else if (this.min.y >= other.max.y) {
        // Up side
        Coordinate(this.max.x, this.min.y)
          .distanceWithOtherCoordinate(Coordinate(other.min.x, other.max.y))
      }
      else {
        other.min.x - this.max.x
      }
    }
    else if (this.min.x >= other.max.x) {
      // Right side
      if (this.max.y <= other.min.y) {
        // Down side
        Coordinate(this.min.x, this.max.y)
          .distanceWithOtherCoordinate(Coordinate(other.max.x, other.min.y))
      }
      else if (this.min.y >= other.max.y) {
        // Up side
        this.min.distanceWithOtherCoordinate(other.max)
      }
      else {
        this.min.x - other.max.x
      }
    }
    else if (this.max.y <= other.min.y) {
      other.min.y - this.max.y
    }

    else if (this.min.y >= other.max.y) {
      this.min.y - other.max.y
    }
    Console.err.println("Wrong min distance computation between two MBRs")
    0.0
  }

  def isIntersectWithOtherMBR(other: MBR): Boolean = {
    val c1 = this.center
    val c2 = other.center
    if (math.abs(c1.x - c2.x) < (this.width / 2 + other.width / 2)
      && math.abs(c1.y - c2.y) < (this.height / 2 + other.height / 2)) {
      true
    }
    else false
  }

}
