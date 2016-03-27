
/**
  * Created by liuzh on 2016/3/13.
  */
package org.apache.spark.graphx.OSR
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

import scala.collection.mutable.ListBuffer

class STRPartitioner(sliceNumPerDim: Int,
                     sampleRate: Double,
                     rdd: RDD[Vertex])
//  extends Partitioner
{
  //  def numPartions: Int = math.pow(sliceNumPerDim, 2).asInstanceOf[Int]
  //
  //  def getPartition(key: Any): Int =  {
  //    val k = key.asInstanceOf[Point]
  //  }

  val sampled = rdd.sample(withReplacement = false, sampleRate, System.currentTimeMillis).collect()
  val sampledNum: Int= sampled.length
  val numPerCol : Int= sampledNum / sliceNumPerDim
  val numPerCell: Int = numPerCol / sliceNumPerDim
  val bounds = ((Double.MinValue, Double.MinValue), (Double.MaxValue, Double.MaxValue))
  val colGroup = sampled.sortWith(_.coordinate._1 < _.coordinate._1 ).grouped(numPerCol).toArray
  var mbrList = ListBuffer[MBR]()
  for(i <- colGroup.indices) {
    val colGroupLength = colGroup.length
    var min_x = 0.0
    var min_y = 0.0
    var max_x = 0.0
    var max_y = 0.0
    if(i == 0 && i != colGroupLength - 1) {
      min_x = bounds._1._1
      max_x = colGroup(i + 1).head.coordinate._1
    } else if(i == 0 && i == colGroupLength - 1) {
      min_x = bounds._1._1
      max_x = bounds._2._1
    } else if(i != 0 && i != colGroupLength - 1) {
      min_x = colGroup(i).head.coordinate._1
      val max_x = colGroup(i + 1).head.coordinate._1
    } else if( i != 0 && i == colGroupLength - 1) {
      min_x = colGroup(i).head.coordinate._1
      max_x = bounds._2._1
    }
    val cellGroup =
      colGroup(i).sortWith(_.coordinate._2 < _.coordinate._2).grouped(numPerCell).toArray
    val cellGroupLength = cellGroup.length
    for( j <- cellGroup.indices) {
      if(j == 0 && j != cellGroupLength - 1) {
        min_y = bounds._1._2
        max_y = cellGroup(j + 1).head.coordinate._2
      } else if(j == 0 && j == cellGroupLength - 1) {
        min_y = bounds._1._2
        max_y = bounds._2._2
      } else if(j != 0 && j != cellGroupLength - 1) {
        min_y = cellGroup(j).head.coordinate._2
        max_y = cellGroup(j + 1).head.coordinate._2
      } else if(j != 0 && j == cellGroupLength - 1) {
        min_y = cellGroup(j).head.coordinate._2
        max_y = bounds._2._2
      }
      mbrList += new MBR((min_x, min_y), (max_x, max_y))
    }
  }
   val mbrArray = mbrList.toArray.zipWithIndex
//   val rt = RTree.buildFromMBRs(mbrArray, 100)
  val x = 0
}
