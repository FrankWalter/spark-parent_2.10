
/**
  * Created by liuzh on 2016/3/13.
  */
package org.apache.spark.graphx.OSR
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
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

  val colGroup = sampled.sortWith(_.coordinate._1 < _.coordinate._1 ).grouped(numPerCol).toArray
  val mbrs = colGroup.flatMap(col => {
    val length = col.length
    val col_min = col.head.coordinate._1
    val col_max = col(length - 1).coordinate._1
    val cellGroup = col.sortWith(_.coordinate._2 < _.coordinate._2).grouped(numPerCell).toArray
    cellGroup.map(cell => {
      val cell_min = cell.head.coordinate._2
      val cell_max = cell(cell.length - 1).coordinate._2
      new MBR((col_min, cell_min),(col_max, cell_max))
    })
  })
  val x = 0
  //  val mbrs = groupVertex(sampled)
}
