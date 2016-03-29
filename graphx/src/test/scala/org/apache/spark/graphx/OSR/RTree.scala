
/**
  * Created by liuzh on 2016/3/27.
  */
package org.apache.spark.graphx.OSR

import scala.collection.mutable.ListBuffer

object RTree {
  def buildFromMBRs(mbrs: List[(MBR, Int)], level: Int): RTree = {
    val sliceNumPerDim: Int = math.ceil(math.pow(mbrs.length, 1 / (level.toDouble * 2))).toInt
    null
  }
  def MBRcenter(mbr: MBR): Point = {
    new Point((mbr.min._1 + mbr.max._1) / 2, (mbr.min._2 + mbr.max._2) / 2)
  }
  def closureMBR(mbrs: List[MBR]): MBR = {
    val min = new Array[Double](2)
    val max = new Array[Double](2)
    val newBound = mbrs
      .map(item => (item.min, item.max))
      .reduce((left, right) =>
        (
          (math.min(left._1._1, right._1._1), math.min(left._1._2, right._1._2)),
          (math.max(left._2._1, right._2._1), math.max(left._2._2, right._2._2))
          )
      )
    new MBR(newBound._1, newBound._2)
  }
  def groupPartition(mbrs: List[(MBR, Int)], sliceNumPerDim: Int): List[RTreeLeaf] = {
    val numPerCol : Int= mbrs.length / sliceNumPerDim
    val numPerCell: Int = numPerCol / sliceNumPerDim
    var leafList = ListBuffer[RTreeLeaf]()
    val colGroup =
      mbrs.sortWith(
        (left, right) => MBRcenter(left._1).x < MBRcenter(right._1).x)
        .grouped(numPerCol).toList
    for(i <- colGroup.indices) {
      val cellGroup =
        colGroup(i).sortWith(
          (left, right) => MBRcenter(left._1).y < MBRcenter(right._1).y)
          .grouped(numPerCell).toList
      for( j <- cellGroup.indices) {
        leafList += new RTreeLeaf(cellGroup(i))
      }
    }
    leafList.toList
  }
}

class RTree(rootPara: RTreeNode) {
  val root: RTreeNode = rootPara
}

class RTreeNode(childPara: List[RTreeNode]) {
  val childNode: List[RTreeNode] = childPara
  val bound: MBR = RTree.closureMBR(childNode.map(node => node.bound))
}

class RTreeLeaf(childPara: List[(MBR, Int)])
  extends RTreeNode(Nil) {
  val childPartition: List[(MBR, Int)] = childPara
  override val bound = RTree.closureMBR(childPartition.map(partition => partition._1))
}
