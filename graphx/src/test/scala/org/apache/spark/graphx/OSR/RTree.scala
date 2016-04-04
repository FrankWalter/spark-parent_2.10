
/**
  * Created by liuzh on 2016/3/27.
  */
package org.apache.spark.graphx.OSR

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RTree {
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
  def MBRIntersectWithVertex(mbr: MBR, vertex: Vertex): Boolean = {
    if (vertex.coordinate._1 >= mbr.min._1
      && vertex.coordinate._2 >= mbr.min._2
      && vertex.coordinate._1 <= mbr.max._1
      && vertex.coordinate._2 <= mbr.max._2) true
    else false
  }
  def MBRcenter(mbr: MBR): Point = {
    new Point((mbr.min._1 + mbr.max._1) / 2, (mbr.min._2 + mbr.max._2) / 2)
  }
  def closureMBR(mbrs: List[MBR]): MBR = {
    val min = new Array[Double](2)
    val max = new Array[Double](2)
    val newBound = mbrs
      .map(item => (item.min, item.max)).aggregate[MBR](null)(
      (mbr, bound) => {
        if(mbr != null) {
          new MBR(
            (math.min(mbr.min._1, bound._1._1), math.min(mbr.min._2, bound._1._2)),
            (math.max(mbr.max._1, bound._2._1), math.max(mbr.max._2, bound._2._2)))
        }
        else new MBR(bound._1, bound._2)
      }, (left, right) => {
        if (left == null) right
        else if (right == null) left
        else {
          new MBR(
            (math.min(left.min._1, right.min._1), math.min(left.min._2, right.min._2)),
            (math.max(left.max._1, right.max._1), math.max(left.max._2, right.max._2)))
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

class RTree(rootPara: RTreeNode) {
  val root: RTreeNode = rootPara
  def pointPartitionQuery(vertex: Vertex): Int = {
    def queue = new mutable.Queue[RTreeNode]()
    queue.enqueue(root)
    var head: RTreeNode = null
    while (queue.nonEmpty) {
      head = queue.dequeue()
      if (head.isInstanceOf[RTreeLeaf]) {
        for (elem <- head.asInstanceOf[RTreeLeaf].childPartitions) {
          if (RTree.MBRIntersectWithVertex(elem._1, vertex)) return elem._2
        }
      }
    }
  }
}

class RTreeNode(childPara: List[RTreeNode]) {
  val childNode: List[RTreeNode] = childPara
  val bound: MBR = RTree.closureMBR(childNode.map(node => node.bound))
}

class RTreeLeaf(childPara: List[(MBR, Int)])
  extends RTreeNode(Nil) {
  val childPartitions: List[(MBR, Int)] = childPara
  override val bound = RTree.closureMBR(childPartitions.map(partition => partition._1))
}
