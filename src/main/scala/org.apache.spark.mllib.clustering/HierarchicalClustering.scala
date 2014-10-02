/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.clustering

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * the configuration for a hierarchical clusterint algorithm
 *
 * @param strategy the strategy to select the next splited node
 * @param numClusters the number of clusters you want
 * @param subIterations the number of iterations at digging
 * @param epsilon
 * @param randomSeed
 */
class HierarchicalClusteringConf(
  private var strategy: ClusterTreeStats,
  private var numClusters: Int,
  private var subIterations: Int,
  private var epsilon: Double,
  private var randomSeed: Int) extends Serializable {

  def this() = this(new ClusterVarianceStats, 100, 20, 10E-6, 1)

  def setStrategy(strategy: ClusterTreeStats): this.type = {
    this.strategy = strategy
    this
  }

  def getStrategy(): ClusterTreeStats = this.strategy

  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  def getNumClusters(): Int = this.numClusters

  def setSubIterations(iterations: Int): this.type = {
    this.subIterations = iterations
    this
  }

  def getSubIterations(): Int = this.subIterations

  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  def getEpsilon(): Double = this.epsilon

  def setRandomSeed(seed: Int): this.type = {
    this.randomSeed = seed
    this
  }

  def getRandomSeed(): Int = this.randomSeed
}


class HierarchicalClustering(val conf: HierarchicalClusteringConf) extends Serializable {

  def this() = this(new HierarchicalClusteringConf())

  def train(data: RDD[Vector]): HierarchicalClusteringModel = {
    data.cache

    val clusterTree = ClusterTree.fromRDD(data)
    val model = new HierarchicalClusteringModel(clusterTree)

    while (model.clusterTree.treeSize() < this.conf.getNumClusters) {
      println(model.clusterTree.treeSize())

      updateAllStats(model.clusterTree)
      val node = nextNode(model.clusterTree)
      val subNodes = split(node)
      node.insert(subNodes.toList)
    }
    updateAllStats(model.clusterTree)
    model
  }

  def nextNode(clusterTree: ClusterTree): ClusterTree = {
    // select the max value stats of clusters which are leafs of a tree
    clusterTree.toSeq().filter(_.isSplitable()).maxBy(_.getStats())
  }

  def updateAllStats(tree: ClusterTree) {
    val strategy = this.conf.getStrategy()
    val trees = tree.toSeq().filter(_.isSplitable()).filter(_.getStats() == None)
    strategy(trees).foreach { case (tree, stats) => tree.setStats(Some(stats))}
  }

  def takeInitCenters(data: RDD[Vector]): Array[Vector] = {
    data.takeSample(false, 2, conf.getRandomSeed) // because of bi-sect ==> 2
  }

  def split(clusterTree: ClusterTree): Array[ClusterTree] = {
    val data = clusterTree.data
    var centers = takeInitCenters(data)
    var finder: ClosestCenterFinder = new EuclideanClosestCenterFinder(centers)

    def relativeError(o: Double, n: Double): Double = Math.abs((o - n) / o)
    def calcTotalStats(array: Array[Vector]): Double =
      array.map(vector => breezeNorm(vector.toBreeze, 2.0)).sum

    var numIter = 0
    var error = Double.MaxValue
    while (error > conf.getEpsilon() && numIter < conf.getSubIterations()) {
      println(s"error = ${error}, epsilon = ${conf.getEpsilon()}")
      val closest = data.mapPartitions { iter =>
        val map = scala.collection.mutable.Map.empty[Int, (BV[Double], Int)]
        iter.foreach { point =>
          val idx = finder(point)
          val (sumBV, n) = map.get(idx).getOrElse((BV.zeros[Double](point.size), 0))
          map(idx) = (sumBV + point.toBreeze, n + 1)
        }
        map.toIterator
      }

      val pointStats = scala.collection.mutable.Map.empty[Int, (BV[Double], Int)]
      closest.collect().foreach { case (key, (point, count)) =>
        val (sumBV, n) = pointStats.get(key).getOrElse((BV.zeros[Double](point.size), 0))
        pointStats(key) = (sumBV + point, n + count)
      }
      val newCenters = pointStats.map { case ((idx: Int, (center: BV[Double], counts: Int))) =>
        Vectors.fromBreeze(center :/ counts.toDouble)
      }.toArray

      error = relativeError(calcTotalStats(centers), calcTotalStats(newCenters))
      centers = newCenters
      numIter += 1
      finder = new EuclideanClosestCenterFinder(centers)
    }

    val closest = data.map(point => (finder(point), point))
    val nodes = centers.zipWithIndex.map { case (center, i) =>
      val subData = closest.filter(_._1 == i).map(_._2)
      new ClusterTree(subData, center)
    }
    nodes
  }
}


object HierarchicalClustering {

  def train(data: RDD[Vector], numClusters: Int): HierarchicalClusteringModel = {
    val conf = new HierarchicalClusteringConf()
        .setNumClusters(numClusters)
    val app = new HierarchicalClustering(conf)
    app.train(data)
  }
}


class HierarchicalClusteringModel(val clusterTree: ClusterTree) extends Serializable

class ClusterTree(
  val data: RDD[Vector],
  val center: Vector,
  private var stats: Option[Double],
  private var dataSize: Option[Double],
  private var children: List[ClusterTree],
  private var parent: Option[ClusterTree]) extends Serializable {

  def this(data: RDD[Vector], center: Vector) =
    this(data, center, None, None, List.empty[ClusterTree], None)

  def toSeq(): Seq[ClusterTree] = {
    this.children.size match {
      case 0 => Seq(this)
      case _ => Seq(this) ++ this.children.map(child => child.toSeq()).flatten
    }
  }

  def depth(): Int = {
    this.parent match {
      case None => 0
      case _ => 1 + this.parent.get.depth()
    }
  }

  def insert(children: List[ClusterTree]): Unit = {
    this.children = this.children ++ children
    children.foreach(child => child.setParent(Some(this)))
  }

  def insert(child: ClusterTree): Unit = insert(List(child))

  def treeSize(): Int = this.toSeq().filter(_.isLeaf()).size

  def getDataSize(): Long = {
    if(this.dataSize == None) {
      this.dataSize = Some(this.data.count())
    }
    this.dataSize.get.toLong
  }

  def setParent(parent: Option[ClusterTree]) = this.parent = parent

  def getParent(): Option[ClusterTree] = this.parent

  def getChildren(): List[ClusterTree] = this.children

  def setStats(stats: Option[Double]) = this.stats = stats

  def getStats(): Option[Double] = this.stats

  def isLeaf(): Boolean = (this.children.size == 0)

  def isSplitable(): Boolean = (this.isLeaf() && this.getDataSize() >= 2)
}

object ClusterTree {

  def fromRDD(data: RDD[Vector]): ClusterTree = {
    val pointStat = data.mapPartitions { iter =>
      val stat = iter.map(v => (v.toBreeze, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      Iterator(stat)
    }.collect().reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    val center = Vectors.fromBreeze(pointStat._1.:/(pointStat._2.toDouble))
    new ClusterTree(data, center)
  }
}

trait ClusterTreeStats extends Function1[Seq[ClusterTree], Map[ClusterTree, Double]] {
  def apply(clusterTrees: Seq[ClusterTree]): Map[ClusterTree, Double]
}

class DataSizeStats extends ClusterTreeStats {

  override def apply(clusterTrees: Seq[ClusterTree]): Map[ClusterTree, Double] = {
    clusterTrees.map(tree => (tree, tree.data.count().toDouble)).toMap
  }
}

class ClusterVarianceStats extends ClusterTreeStats {

  override def apply(clusterTrees: Seq[ClusterTree]): Map[ClusterTree, Double] = {
    clusterTrees.map(tree => (tree, calculate(tree.data))).toMap
  }

  def calculate(data: RDD[Vector]): Double = {
    val first = data.first()
    val eachStats = data.mapPartitions { iter =>
      var n = 0.0
      var mean = Vectors.zeros(first.size).toBreeze
      var M2 = Vectors.zeros(first.size).toBreeze
      iter.map(point => point.toBreeze).foreach { point =>
        n += 1.0
        val delta = point - mean
        mean = mean + delta.:/(n)
        M2 = M2 + delta.:*(n - 1.0) / delta.:*(n)
      }
      val variance = M2.:/(n - 1.0)
      Iterator((mean, variance, n))
    }

    val (mean, variance, n) = eachStats.reduce { case ((meanA, varA, nA), (meanB, varB, nB)) =>
      val nAB = nA + nB
      val meanAB = (meanA.:*(nA) + meanB.:*(nB)).:/(nAB)
      val varAB = (((varA.:*(nA)) + (varB.:*(nB))).:/(nAB)) +
          breezeNorm(((meanB - meanA).:/(nAB)).:*(nA * nB), 2.0)
      (meanAB, varAB, nAB)
    }
    Math.sqrt(variance.map(Math.pow(_, 2.0)).fold(0.0)(_ + _))
  }
}
