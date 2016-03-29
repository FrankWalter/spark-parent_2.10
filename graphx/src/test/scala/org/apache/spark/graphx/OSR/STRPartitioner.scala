
/**
  * Created by liuzh on 2016/3/13.
  */
package org.apache.spark.graphx.OSR
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

import scala.collection.mutable.ListBuffer
object STRPartitioner {
  def apply(sliceNumPerDim: Int,
            sampleRate: Double,
            rdd: RDD[Vertex]): Unit= {
    val bound = computeBound(rdd)
    val partitioner = new STRPartitioner(sliceNumPerDim, sampleRate, bound, rdd)
  }
  def computeBound(rdd: RDD[Vertex]): MBR = {
    val margin = 10
    val newBound = rdd
      .map(item => item.coordinate)
      .aggregate[MBR](null)(
        (mbr, vertex) => {
          if(mbr != null) {
            new MBR(
              (math.min(mbr.min._1, vertex._1), math.min(mbr.min._2, vertex._2)),
              (math.max(mbr.max._1, vertex._1), math.max(mbr.max._2, vertex._2))
            )
          } else {
            new MBR(
              (vertex._1, vertex._2),
              (vertex._1, vertex._2)
            )
          }
        }
          , (left, right) => {
        if(left == null) right
        else if(right == null) left
        else {
          new MBR(
            (math.min(left.min._1, right.min._1), math.min(left.min._2, right.min._2)),
            (math.max(left.max._1, right.max._1), math.max(left.max._2, right.max._2)))
        }
      }
    )
    new MBR(
      (newBound.min._1 - margin, newBound.min._2 - margin),
      (newBound.max._1 + margin, newBound.max._2 + margin))
  }
}
class STRPartitioner(sliceNumPerDim: Int,
                     sampleRate: Double,
                     bound: MBR,
                     rdd: RDD[Vertex])
//  extends Partitioner
{
  //  def numPartions: Int = math.pow(sliceNumPerDim, 2).asInstanceOf[Int]
  //
  //  def getPartition(key: Any): Int =  {
  //    val k = key.asInstanceOf[Point]
  //  }

  def groupVertexes(): Array[(MBR, Int)] = {
    val sampled = rdd.sample(withReplacement = false,
      sampleRate, System.currentTimeMillis).collect()
    val numPerCol : Int= sampled.length / sliceNumPerDim
    val numPerCell: Int = numPerCol / sliceNumPerDim
    val colGroup = sampled.sortWith(_.coordinate._1 < _.coordinate._1 ).grouped(numPerCol).toArray
    var mbrList = ListBuffer[MBR]()
    for(i <- colGroup.indices) {
      val colGroupLength = colGroup.length
      var min_x = 0.0
      var min_y = 0.0
      var max_x = 0.0
      var max_y = 0.0
      if(i == 0 && i != colGroupLength - 1) {
        min_x = bound.min._1
        max_x = colGroup(i + 1).head.coordinate._1
      } else if(i == 0 && i == colGroupLength - 1) {
        min_x = bound.min._1
        max_x = bound.max._1
      } else if(i != 0 && i != colGroupLength - 1) {
        min_x = colGroup(i).head.coordinate._1
        val max_x = colGroup(i + 1).head.coordinate._1
      } else if( i != 0 && i == colGroupLength - 1) {
        min_x = colGroup(i).head.coordinate._1
        max_x = bound.max._1
      }
      val cellGroup =
        colGroup(i).sortWith(_.coordinate._2 < _.coordinate._2).grouped(numPerCell).toArray
      val cellGroupLength = cellGroup.length
      for( j <- cellGroup.indices) {
        if(j == 0 && j != cellGroupLength - 1) {
          min_y = bound.min._2
          max_y = cellGroup(j + 1).head.coordinate._2
        } else if(j == 0 && j == cellGroupLength - 1) {
          min_y = bound.min._2
          max_y = bound.max._2
        } else if(j != 0 && j != cellGroupLength - 1) {
          min_y = cellGroup(j).head.coordinate._2
          max_y = cellGroup(j + 1).head.coordinate._2
        } else if(j != 0 && j == cellGroupLength - 1) {
          min_y = cellGroup(j).head.coordinate._2
          max_y = bound.max._2
        }
        mbrList += new MBR((min_x, min_y), (max_x, max_y))
      }
    }
    val mbrArray = mbrList.toList.zipWithIndex
//    mbrArray.map(item => println(
//      item._1.min.toString().replace("(", "").replace(")", "").replace(",", " ")
//        + item._1.max.toString().replace("(", "").replace(")", "").replace(",", " ")))
    mbrArray
  }
  val mbrArray = groupVertexes()
  //   val rt = RTree.buildFromMBRs(mbrArray, 100)
  val x = 0
}
