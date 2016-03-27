
/**
  * Created by liuzh on 2016/3/27.
  */
package org.apache.spark.graphx.OSR
object RTree {
  def buildFromMBRs(mbrs: Array[(MBR, Int)], maxChildNumPerNode: Int): RTree {

  }
}

class RTree(rootPara: RTreeNode) {
  val root = rootPara
}

class RTreeNode(childPara: List[RTreeNode], boundPara: ((Double, Double), (Double, Double))) {
  val child = childPara
  val bound = boundPara
}

class RTreeLeaf(childPara: List[MBR],
                boundPara: ((Double, Double), (Double, Double)))
  extends RTreeNode(Nil, boundPara) {

  override val child = childPara
}
