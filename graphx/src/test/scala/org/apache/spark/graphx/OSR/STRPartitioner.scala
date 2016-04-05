
/**
  * Created by liuzh on 2016/3/13.
  */
package org.apache.spark.graphx.OSR
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.{SparkConf, Partitioner}
import org.apache.spark.sql.catalyst.expressions._

import scala.collection.mutable.ListBuffer
object STRPartitioner {
  def apply(expectedParNum: Int,
            sampleRate: Double,
            rdd: RDD[Vertex]): Unit= {
    val bound = computeBound(rdd)
    val partitioner = new STRPartitioner(expectedParNum, sampleRate, bound, rdd)
    rdd.partitionBy()
  }
  def computeBound(rdd: RDD[Vertex]): MBR = {
    val margin = 10
    val newBound = rdd
      .map(item => item.coordinate)
      .aggregate[MBR](null)(
      (mbr, coordinate) => {
        if(mbr != null) {
          new MBR(
            Coordinate(math.min(mbr.min.x, coordinate.x), math.min(mbr.min.y, coordinate.y)),
            Coordinate(math.max(mbr.max.x, coordinate.x), math.max(mbr.max.y, coordinate.y))
          )
        } else {
          new MBR(
            Coordinate(coordinate.x, coordinate.y),
            Coordinate(coordinate.x, coordinate.y)
          )
        }
      }
      , (left, right) => {
        if(left == null) right
        else if(right == null) left
        else {
          new MBR(
            Coordinate(math.min(left.min.x, right.min.x), math.min(left.min.y, right.min.y)),
            Coordinate(math.max(left.max.x, right.max.x), math.max(left.max.y, right.max.y)))
        }
      }
    )
    new MBR(
      Coordinate(newBound.min.x - margin, newBound.min.y - margin),
      Coordinate(newBound.max.x + margin, newBound.max.y + margin))
  }
}
class STRPartitioner(expectedParNum: Int,
                     sampleRate: Double,
                     bound: MBR,
                     rdd: RDD[Vertex])
  extends Partitioner
{
  def numPartitions: Int = mbrListWithIndex.length

  def getPartition(key: Any): Int =  {
    val k = key.asInstanceOf[Vertex]
    rt.coorPartitionQuery(k.coordinate)
  }

  def groupVertexes(): List[(MBR, Int)] = {
    val sampled = rdd.sample(withReplacement = false,
      sampleRate, System.currentTimeMillis).collect()
    val sliceNumX: Int = math.ceil(math.sqrt(expectedParNum)).toInt
    val sliceNumY: Int = math.ceil(expectedParNum / sliceNumX.toDouble).toInt
    val numPerCol : Int= math.ceil(sampled.length / sliceNumX.toDouble).toInt
    val numPerCell: Int = math.ceil(numPerCol / sliceNumY.toDouble).toInt
    val colGroup = sampled.sortWith(_.coordinate.x < _.coordinate.x ).grouped(numPerCol).toArray
    var mbrList = ListBuffer[MBR]()
    for(i <- colGroup.indices) {
      val colGroupLength = colGroup.length
      var min_x = 0.0
      var min_y = 0.0
      var max_x = 0.0
      var max_y = 0.0
      if(i == 0 && i != colGroupLength - 1) {
        min_x = bound.min.x
        max_x = colGroup(i + 1).head.coordinate.x
      } else if(i == 0 && i == colGroupLength - 1) {
        min_x = bound.min.x
        max_x = bound.max.x
      } else if(i != 0 && i != colGroupLength - 1) {
        min_x = colGroup(i).head.coordinate.x
        val max_x = colGroup(i + 1).head.coordinate.x
      } else if( i != 0 && i == colGroupLength - 1) {
        min_x = colGroup(i).head.coordinate.x
        max_x = bound.max.x
      }
      val cellGroup =
        colGroup(i).sortWith(_.coordinate.y < _.coordinate.y).grouped(numPerCell).toArray
      val cellGroupLength = cellGroup.length
      for( j <- cellGroup.indices) {
        if(j == 0 && j != cellGroupLength - 1) {
          min_y = bound.min.y
          max_y = cellGroup(j + 1).head.coordinate.y
        } else if(j == 0 && j == cellGroupLength - 1) {
          min_y = bound.min.y
          max_y = bound.max.y
        } else if(j != 0 && j != cellGroupLength - 1) {
          min_y = cellGroup(j).head.coordinate.y
          max_y = cellGroup(j + 1).head.coordinate.y
        } else if(j != 0 && j == cellGroupLength - 1) {
          min_y = cellGroup(j).head.coordinate.y
          max_y = bound.max.y
        }
        mbrList += new MBR(Coordinate(min_x, min_y), Coordinate(max_x, max_y))
      }
    }
    mbrList.toList.zipWithIndex
  }
  val mbrListWithIndex = groupVertexes()
  val rt = RTree.buildFromMBRs(mbrListWithIndex, 2)
  val x = rt.coorPartitionQuery(Coordinate(10, 60))
}
