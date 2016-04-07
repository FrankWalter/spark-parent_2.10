
/**
  * Created by liuzh on 2016/3/13.
  */
package org.apache.spark.graphx.OSR
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, Partitioner}

import scala.collection.mutable.ListBuffer
object STRPartitioner extends Serializable{
  def apply(expectedParNum: Int,
            sampleRate: Double,
            originRdd: RDD[Vertex]): (RDD[(Coordinate, Vertex)], List[(MBR, Int)]) = {
    val bound = computeBound(originRdd)
    val rdd = originRdd.mapPartitions(
      iter => iter.map(row => {
        val cp = row
        (cp.coordinate, cp)
      }
      )
    )
    val tmp = rdd.collect()
    val partitioner = new STRPartitioner(expectedParNum, sampleRate, bound, rdd)
    val shuffled = new ShuffledRDD[Coordinate, Vertex, Vertex](rdd, partitioner)
    shuffled.setSerializer(new KryoSerializer(new SparkConf(false)))
    (shuffled, partitioner.mbrListWithIndex)
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
                     rdd: RDD[_ <: Product2[Coordinate, Vertex]])
  extends Partitioner with  Serializable
{
  def numPartitions: Int = mbrListWithIndex.length

  def getPartition(key: Any): Int =  {
    val k = key.asInstanceOf[Coordinate]
    rt.coorPartitionQuery(k)
  }

  def groupVertexes(): List[(MBR, Int)] = {
    val sampled = rdd.sample(withReplacement = false,
      sampleRate, System.currentTimeMillis).map(_._1).collect()
    val sliceNumX: Int = math.ceil(math.sqrt(expectedParNum)).toInt
    val sliceNumY: Int = math.ceil(expectedParNum / sliceNumX.toDouble).toInt
    val numPerCol : Int= math.ceil(sampled.length / sliceNumX.toDouble).toInt
    val numPerCell: Int = math.ceil(numPerCol / sliceNumY.toDouble).toInt
    val colGroup = sampled.sortWith(_.x < _.x ).grouped(numPerCol).toArray
    var mbrList = ListBuffer[MBR]()
    for(i <- colGroup.indices) {
      val colGroupLength = colGroup.length
      var min_x = 0.0
      var min_y = 0.0
      var max_x = 0.0
      var max_y = 0.0
      if(i == 0 && i != colGroupLength - 1) {
        min_x = bound.min.x
        max_x = colGroup(i + 1).head.x
      } else if(i == 0 && i == colGroupLength - 1) {
        min_x = bound.min.x
        max_x = bound.max.x
      } else if(i != 0 && i != colGroupLength - 1) {
        min_x = colGroup(i).head.x
        val max_x = colGroup(i + 1).head.x
      } else if( i != 0 && i == colGroupLength - 1) {
        min_x = colGroup(i).head.x
        max_x = bound.max.x
      }
      val cellGroup =
        colGroup(i).sortWith(_.y < _.y).grouped(numPerCell).toArray
      val cellGroupLength = cellGroup.length
      for( j <- cellGroup.indices) {
        if(j == 0 && j != cellGroupLength - 1) {
          min_y = bound.min.y
          max_y = cellGroup(j + 1).head.y
        } else if(j == 0 && j == cellGroupLength - 1) {
          min_y = bound.min.y
          max_y = bound.max.y
        } else if(j != 0 && j != cellGroupLength - 1) {
          min_y = cellGroup(j).head.y
          max_y = cellGroup(j + 1).head.y
        } else if(j != 0 && j == cellGroupLength - 1) {
          min_y = cellGroup(j).head.y
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
