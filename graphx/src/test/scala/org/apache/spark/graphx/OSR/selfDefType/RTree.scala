
/**
  * Created by liuzh on 2016/3/27.
  */
package org.apache.spark.graphx.OSR.selfDefType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RTree extends Serializable{
  def buildFromMBRs(mbrs: List[(MBR, Int)], expChildNum: Int): RTree = {
    val sliceNumX: Int = math.ceil(math.sqrt(expChildNum)).toInt
    val sliceNumY: Int = math.ceil(expChildNum / sliceNumX.toDouble).toInt
    val leaves = groupPartitions(mbrs, expChildNum)
    if (leaves.length <= 1) return new RTree(new RTreeNode(leaves))
    var tmpNodes = groupLeaves(leaves, expChildNum)
    var numToBeGrouped = tmpNodes.length
    while (numToBeGrouped > 1) {
      tmpNodes = groupTreeNodes(tmpNodes, expChildNum)
      numToBeGrouped = tmpNodes.length
    }
    new RTree(tmpNodes.head)
  }
  def MBRIntersectWithCoordinate(mbr: MBR, coor: Coordinate): Boolean = {
    if (coor.x >= mbr.min.x
      && coor.y >= mbr.min.y
      && coor.x < mbr.max.x
      && coor.y < mbr.max.y) {
      true
    }
    else false
  }
  def MBRcenter(mbr: MBR): Point = {
    new Point((mbr.min.x + mbr.max.x) / 2, (mbr.min.y + mbr.max.y) / 2)
  }
  def closureMBR(mbrs: List[MBR]): MBR = {
    val min = new Array[Double](2)
    val max = new Array[Double](2)
    val newBound = mbrs
      .map(item => (item.min, item.max)).aggregate[MBR](null)(
      (mbr, bound) => {
        if(mbr != null) {
          new MBR(
            Coordinate(math.min(mbr.min.x, bound._1.x), math.min(mbr.min.y, bound._1.y)),
            Coordinate(math.max(mbr.max.x, bound._2.x), math.max(mbr.max.y, bound._2.y)))
        }
        else new MBR(bound._1, bound._2)
      }, (left, right) => {
        if (left == null) right
        else if (right == null) left
        else {
          new MBR(
            Coordinate(math.min(left.min.x, right.min.x), math.min(left.min.y, right.min.y)),
            Coordinate(math.max(left.max.x, right.max.x), math.max(left.max.y, right.max.y)))
        }
      })
    newBound
  }

  def groupLeaves(mbrs: List[RTreeLeaf], expChildNum: Int): List[RTreeNode] = {
    val expNodeNum = mbrs.length / expChildNum
    val sliceNumX: Int = math.ceil(math.sqrt(expNodeNum)).toInt
    val sliceNumY: Int = math.ceil(expNodeNum / sliceNumX.toDouble).toInt
    val numPerCol : Int= mbrs.length / sliceNumX
    val numPerCell: Int = numPerCol / sliceNumY
    var nodeList = ListBuffer[RTreeNode]()
    if (numPerCol == 0) {
      nodeList += new RTreeNode(mbrs)
      return nodeList.toList
    }
    val colGroup =
      mbrs.sortWith(
        (left, right) => MBRcenter(left.bound).x < MBRcenter(right.bound).x)
        .grouped(numPerCol).toList
    if (numPerCell == 0) {
      for (i <- colGroup.indices) {
        nodeList += new RTreeNode(colGroup(i))
      }
      return nodeList.toList
    }
    for(i <- colGroup.indices) {
      val cellGroup =
        colGroup(i).sortWith(
          (left, right) => MBRcenter(left.bound).y < MBRcenter(right.bound).y)
          .grouped(numPerCell).toList
      for( j <- cellGroup.indices) {
        nodeList += new RTreeNode(cellGroup(j))
      }
    }
    nodeList.toList
  }
  def groupTreeNodes(mbrs: List[RTreeNode], expChildNum: Int): List[RTreeNode] = {
    val expNodeNum = mbrs.length / expChildNum
    val sliceNumX: Int = math.ceil(math.sqrt(expNodeNum)).toInt
    val sliceNumY: Int = math.ceil(expNodeNum / sliceNumX.toDouble).toInt
    val numPerCol : Int= mbrs.length / sliceNumX
    val numPerCell: Int = numPerCol / sliceNumY
    var nodeList = ListBuffer[RTreeNode]()
    if (numPerCol == 0) {
      nodeList += new RTreeNode(mbrs)
      return nodeList.toList
    }
    val colGroup =
      mbrs.sortWith(
        (left, right) => MBRcenter(left.bound).x < MBRcenter(right.bound).x)
        .grouped(numPerCol).toList
    if (numPerCell == 0) {
      for (i <- colGroup.indices) {
        nodeList += new RTreeNode(colGroup(i))
      }
      return nodeList.toList
    }
    for(i <- colGroup.indices) {
      val cellGroup =
        colGroup(i).sortWith(
          (left, right) => MBRcenter(left.bound).y < MBRcenter(right.bound).y)
          .grouped(numPerCell).toList
      for( j <- cellGroup.indices) {
        nodeList += new RTreeNode(cellGroup(j))
      }
    }
    nodeList.toList
  }

  def groupPartitions(mbrs: List[(MBR, Int)], expChildNum: Int): List[RTreeLeaf] = {
    val expNodeNum = mbrs.length / expChildNum
    val sliceNumX: Int = math.ceil(math.sqrt(expNodeNum)).toInt
    val sliceNumY: Int = math.ceil(expNodeNum / sliceNumX.toDouble).toInt
    val numPerCol : Int= mbrs.length / sliceNumX
    val numPerCell: Int = numPerCol / sliceNumY
    var leafList = ListBuffer[RTreeLeaf]()
    if (numPerCol == 0) {
      leafList += new RTreeLeaf(mbrs)
      return leafList.toList
    }
    val colGroup =
      mbrs.sortWith(
        (left, right) => MBRcenter(left._1).x < MBRcenter(right._1).x)
        .grouped(numPerCol).toList
    if (numPerCell == 0) {
      for (i <- colGroup.indices) {
        leafList += new RTreeLeaf(colGroup(i))
      }
      return leafList.toList
    }
    for(i <- colGroup.indices) {
      val cellGroup =
        colGroup(i).sortWith(
          (left, right) => MBRcenter(left._1).y < MBRcenter(right._1).y)
          .grouped(numPerCell).toList
      for( j <- cellGroup.indices) {
        leafList += new RTreeLeaf(cellGroup(j))
      }
    }
    leafList.toList
  }
}

class RTree(rootPara: RTreeNode) extends Serializable{
  val root: RTreeNode = rootPara
  def coorPartitionQuery(coor: Coordinate): Int = {
    val queue = new mutable.Queue[RTreeNode]()
    queue.enqueue(root)
    var head: RTreeNode = null
    while (queue.nonEmpty) {
      head = queue.dequeue()
      head match {
        case leaf: RTreeLeaf =>
          for (elem <- leaf.childPartitions) {
            if (RTree.MBRIntersectWithCoordinate(elem._1, coor)) return elem._2
          }
        case node: RTreeNode =>
          for (elem <- node.childNode) {
            queue.enqueue(elem)
          }
        case _ => Unit
      }
    }
    -1
  }
}

class RTreeNode(childPara: List[RTreeNode]) extends Serializable{
  val childNode: List[RTreeNode] = childPara
  val bound: MBR = RTree.closureMBR(childNode.map(node => node.bound))
}

class RTreeLeaf(childPara: List[(MBR, Int)])
  extends RTreeNode(Nil) with Serializable{
  val childPartitions: List[(MBR, Int)] = childPara
  override val bound = RTree.closureMBR(childPartitions.map(partition => partition._1))
}
