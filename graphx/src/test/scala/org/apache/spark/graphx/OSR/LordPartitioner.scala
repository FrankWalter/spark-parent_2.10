
/**
  * Created by liuzh on 2016/3/13.
  */
package org.apache.spark.graphx.OSR

import org.apache.spark.Partitioner
class LordPartitioner(partitions: Int) extends Partitioner {
  def numPartions: Int = partitions
  def getPartition(key: Any): Int = key match {

  }
}
