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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => breezeNorm}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{Logging, SparkException}

import scala.collection.{Map, mutable}


/**
 * Top-level methods for calling the hierarchical clustering algorithm
 */
object HierarchicalClustering extends Logging {

  private[clustering] val ROOT_INDEX_KEY = 1

  /**
   * Trains a hierarchical clustering model with the given data
   *
   * @param data trained data
   * @param numClusters the maximum number of clusters you want
   * @return a hierarchical clustering model
   */
  def train(data: RDD[Vector], numClusters: Int): HierarchicalClusteringModel = {
    val algo = new HierarchicalClustering().setNumClusters(numClusters)
    algo.run(data)
  }

  /**
   * Trains a hierarchical clustering model with the given data
   *
   * @param data training data
   * @param numClusters the maximum number of clusters you want
   * @param subIterations the number of sub-iterations
   * @param maxRetries the number of maximum retries when the clustering can't be succeeded
   * @param seed the randomseed to generate the initial vectors for each bisecting
   * @return a hierarchical clustering model
   */
  def train(data: RDD[Vector],
    numClusters: Int,
    subIterations: Int,
    maxRetries: Int,
    seed: Int): HierarchicalClusteringModel = {

    val algo = new HierarchicalClustering().setNumClusters(numClusters)
        .setSubIterations(subIterations)
        .setMaxRetries(maxRetries)
        .setSeed(seed)
    algo.run(data)
  }

  /**
   * Finds the closes cluster's center
   *
   * @param metric a distance metric
   * @param centers centers of the clusters
   * @param point a target point
   * @return an index of the array of clusters
   */
  private[mllib]
  def findClosestCenter(metric: Function2[BV[Double], BV[Double], Double])
        (centers: Array[BV[Double]])(point: BV[Double]): Int = {
    val (closestCenter, closestIndex) =
      centers.zipWithIndex.map { case (center, idx) => (metric(center, point), idx)}.minBy(_._1)
    closestIndex
  }
}

/**
 * This is a divisive hierarchical clustering algorithm based on bi-sect k-means algorithm.
 *
 * The main idea of this algorithm is based on "A comparison of document clustering techniques",
 * M. Steinbach, G. Karypis and V. Kumar. Workshop on Text Mining, KDD, 2000.
 * http://cs.fit.edu/~pkc/classes/ml-internet/papers/steinbach00tr.pdf
 *
 * @param numClusters tne number of clusters you want
 * @param clusterMap the pairs of cluster and its index as Map
 * @param subIterations the number of sub-iterations
 * @param maxRetries the number of maximum retries
 * @param seed a random seed
 */
class HierarchicalClustering(
  private var numClusters: Int,
  private var clusterMap: Map[Int, ClusterTree],
  private var subIterations: Int,
  private var maxRetries: Int,
  private var seed: Int) extends Logging {

  /**
   * Constructs with the default configuration
   */
  def this() = this(20, mutable.ListMap.empty[Int, ClusterTree], 20, 10, 1)

  /**
   * Sets the number of clusters you want
   */
  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  /**
   * Sets the number of sub-iterations in each clustering step
   */
  def setSubIterations(subIterations: Int): this.type = {
    this.subIterations = subIterations
    this
  }

  def getSubIterations(): Int = this.subIterations

  /**
   * Sets the number of maximum retries of each clustering step
   */
  def setMaxRetries(maxRetries: Int): this.type = {
    this.maxRetries = maxRetries
    this
  }

  def getMaxRetries(): Int = this.maxRetries

  /**
   * Sets the random seed
   */
  def setSeed(seed: Int): this.type = {
    this.seed = seed
    this
  }

  def getSeed(): Int = this.seed

  /**
   * Runs the hierarchical clustering algorithm
   * @param input RDD of vectors
   * @return model for the hierarchical clustering
   */
  def run(input: RDD[Vector]): HierarchicalClusteringModel = {
    val sc = input.sparkContext
    log.info(s"${sc.appName} starts a hierarchical clustering algorithm")

    var data = initData(input).cache()
    val startTime = System.currentTimeMillis()

    // `clusters` is described as binary tree structure
    // `clusters(1)` means the root of a binary tree
    var clusters = summarizeAsClusters(data)
    var leafClusters = clusters
    var step = 1
    var numDividedClusters = 0
    var noMoreDividable = false
    val maxAllNodesInTree = 2 * this.numClusters - 1
    while (clusters.size < maxAllNodesInTree && noMoreDividable == false) {
      log.info(s"${sc.appName} starts step ${step}")

      // enough to be clustered if the number of divided clusters is equal to 0
      val divided = getDividedClusters(data, leafClusters)
      if (divided.size == 0) {
        noMoreDividable = true
      }
      else {
        // update each index
        val newData = updateClusterIndex(data, divided).cache()
        data.unpersist()
        data = newData.cache()

        // merge the divided clusters with the map as the cluster tree
        clusters = clusters ++ divided
        numDividedClusters = data.map(_._1).distinct().count().toInt
        leafClusters = divided
        step += 1

        log.info(s"${sc.appName} adding ${divided.size} new clusters at step:${step}")
      }
    }

    log.info(s"Building the cluster tree is started in ${sc.appName}")
    val root = buildTree(clusters, HierarchicalClustering.ROOT_INDEX_KEY, this.numClusters)
    if (root == None) {
      new SparkException("Failed to build a cluster tree from a Map type of clusters")
    }
    val finishTime = (System.currentTimeMillis() - startTime) / 1000.0
    log.info(s"Elapsed Time for Hierarchical Clustering Training: ${finishTime} [sec]")
    new HierarchicalClusteringModel(root.get)
  }

  /**
   * Assigns the initial cluster index id to all data
   */
  private[clustering]
  def initData(data: RDD[Vector]): RDD[(Int, BV[Double])] = {
    data.map { v: Vector => (HierarchicalClustering.ROOT_INDEX_KEY, v.toBreeze)}.cache
  }

  /**
   * Summarizes data by each cluster as ClusterTree2 classes
   */
  private[clustering]
  def summarizeAsClusters(data: RDD[(Int, BV[Double])]): Map[Int, ClusterTree] = {
    // summarize input data
    val stats = summarize(data)

    // convert statistics to ClusterTree class
    stats.map { case (i, (sum, n, sumOfSquares)) =>
      val center = Vectors.fromBreeze(sum :/ n)
      val variances = n match {
        case n if n > 1 => Vectors.fromBreeze(sumOfSquares.:*(n) - (sum :* sum) :/ (n * (n - 1.0)))
        case _ => Vectors.zeros(sum.size)
      }
      (i, new ClusterTree(center, n.toLong, variances))
    }.toMap
  }

  /**
   * Summarizes data by each cluster as Map
   */
  private[clustering]
  def summarize(data: RDD[(Int, BV[Double])]): Map[Int, (BV[Double], Double, BV[Double])] = {
    data.mapPartitions { iter =>
      // calculate the accumulation of the all point in a partition and count the rows
      val map = mutable.Map.empty[Int, (BV[Double], Double, BV[Double])]
      iter.foreach { case (idx: Int, point: BV[Double]) =>
        // get a map value or else get a sparse vector
        val (sumBV, n, sumOfSquares) = map.get(idx)
            .getOrElse(BSV.zeros[Double](point.size), 0.0, BSV.zeros[Double](point.size))
        map(idx) = (sumBV + point, n + 1.0, sumOfSquares + (point :* point))
      }
      map.toIterator
    }.reduceByKey { case ((sum1, n1, sumOfSquares1), (sum2, n2, sumOfSquares2)) =>
      // sum the accumulation and the count in the all partition
      (sum1 + sum2, n1 + n2, sumOfSquares1 + sumOfSquares2)
    }.collect().toMap
  }

  /**
   * Gets the initial centers for bi-sect k-means
   */
  private[clustering]
  def initChildrenCenter(clusters: Map[Int, BV[Double]]): Map[Int, BV[Double]] = {
    val rand = new XORShiftRandom()
    rand.setSeed(this.seed.toLong)

    clusters.flatMap { case (idx, center) =>
      val childrenIndexes = Array(2 * idx, 2 * idx + 1)
      val relativeErrorCoefficient = 0.001
      Array(
        (2 * idx, center.map(elm => elm - (elm * relativeErrorCoefficient * rand.nextDouble()))),
        (2 * idx + 1, center.map(elm => elm + (elm * relativeErrorCoefficient * rand.nextDouble())))
      )
    }.toMap
  }

  /**
   * Gets the new divided centers
   */
  private[clustering]
  def getDividedClusters(data: RDD[(Int, BV[Double])],
    dividedClusters: Map[Int, ClusterTree]): Map[Int, ClusterTree] = {
    val sc = data.sparkContext
    val appName = sc.appName

    // get keys of dividable clusters
    val dividableKeys = dividedClusters.filter { case (idx, cluster) =>
      cluster.variances.toArray.sum > 0.0 && cluster.records >= 2
    }.keySet
    if (dividableKeys.size == 0) {
      log.info(s"There is no dividable clusters in ${appName}.")
      return Map.empty[Int, ClusterTree]
    }

    // divide input data
    var dividableData = data.filter { case (idx, point) => dividableKeys.contains(idx)}
    val idealIndexes = dividableKeys.flatMap(idx => Array(2 * idx, 2 * idx + 1).toIterator)
    var stats = divide(data, dividedClusters)

    // if there is clusters which is failed to be divided,
    // retry to divide only failed clusters again and again
    var tryTimes = 1
    while (stats.size != dividableKeys.size * 2 && tryTimes <= this.maxRetries) {

      // get the indexes of clusters which is failed to be divided
      val failedIndexes = idealIndexes.filterNot(stats.keySet.contains).map(idx => (idx / 2).toInt)
      val failedCenters = dividedClusters.filter { case (idx, clstr) => failedIndexes.contains(idx)}
      log.info(s"# failed clusters is ${failedCenters.size} of ${dividableKeys.size}" +
          s"at ${tryTimes} times in ${appName}")

      // divide the failed clusters again
      sc.broadcast(failedIndexes)
      dividableData = data.filter { case (idx, point) => failedIndexes.contains(idx)}
      val missingStats = divide(dividableData, failedCenters)
      stats = stats ++ missingStats
      tryTimes += 1
    }

    // make children clusters
    stats.filter { case (i, (sum, n, sumOfSquares)) => n > 0}
        .map { case (i, (sum, n, sumOfSquares)) =>
      val center = Vectors.fromBreeze(sum :/ n)
      val variances = Vectors.fromBreeze(sumOfSquares.:*(n) - (sum :* sum) :/ (n * (n - 1.0)))
      val child = new ClusterTree(center, n.toLong, variances)
      (i, child)
    }.toMap
  }

  /**
   * Builds a cluster tree from a Map of clusters
   *
   * @param treeMap divided clusters as a Map class
   * @param rootIndex index you want to start
   * @param numClusters the number of clusters you want
   * @return
   */
  private[clustering]
  def buildTree(treeMap: Map[Int, ClusterTree],
    rootIndex: Int,
    numClusters: Int): Option[ClusterTree] = {

    // if there is no index in the Map
    if (!treeMap.contains(rootIndex)) return None

    // build cluster tree until queue is empty or until the number of clusters is enough
    val root = treeMap(rootIndex)
    var queue = Map(rootIndex -> root)
    while (queue.size > 0 && root.getLeavesNodes().size < numClusters) {
      // pick up the cluster whose variance is the maximum in the queue
      val mostScattered = queue.maxBy(_._2.getSumOfSquaresVariances())
      val mostScatteredKey = mostScattered._1
      val mostScatteredCluster = mostScattered._2

      // relate the most scattered cluster to its children clusters
      val childrenIndexes = Array(2 * mostScatteredKey, 2 * mostScatteredKey + 1)
      if (childrenIndexes.forall(i => treeMap.contains(i))) {
        // insert children to the most scattered cluster
        val children = childrenIndexes.map(i => treeMap(i))
        mostScatteredCluster.insert(children)

        // calculate the local dendrogram height
        // TODO Supports distance metrics other Euclidean distance metric
        val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
        val localHeight = children
            .map(child => metric(child.center.toBreeze, mostScatteredCluster.center.toBreeze)).max
        mostScatteredCluster.setLocalHeight(localHeight)

        // update the queue
        queue = queue ++ childrenIndexes.map(i => (i -> treeMap(i))).toMap
      }

      // remove the cluster which is involved to the cluster tree
      queue = queue.filterNot(_ == mostScattered)

      log.info(s"Total Clusters: ${root.getLeavesNodes().size} / ${numClusters}. " +
          s"Cluster ${childrenIndexes.mkString(",")} are merged.")
    }
    Some(root)
  }

  /**
   * Divides the input data
   *
   * @param data the pairs of cluster index and point which you want to divide
   * @param clusters the clusters you want to divide AS a Map class
   * @return divided clusters as Map
   */
  private[clustering]
  def divide(data: RDD[(Int, BV[Double])],
    clusters: Map[Int, ClusterTree]): Map[Int, (BV[Double], Double, BV[Double])] = {

    val sc = data.sparkContext
    val centers = clusters.map { case (idx, cluster) => (idx, cluster.center.toBreeze)}
    var newCenters = initChildrenCenter(centers)
    if (newCenters.size == 0) {
      return Map.empty[Int, (BV[Double], Double, BV[Double])]
    }
    sc.broadcast(newCenters)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    sc.broadcast(metric)

    val vectorSize = newCenters(newCenters.keySet.min).size
    var stats = newCenters.keys.map { idx =>
      (idx, (BSV.zeros[Double](vectorSize).toVector, 0.0, BSV.zeros[Double](vectorSize).toVector))
    }.toMap

    var subIter = 0
    var diffVariances = Double.MaxValue
    var oldVariances = Double.MaxValue
    var variances = Double.MaxValue
    while (subIter < this.subIterations && diffVariances > 10E-4) {
      // calculate summary of each cluster
      val eachStats = data.mapPartitions { iter =>
        val map = mutable.Map.empty[Int, (BV[Double], Double, BV[Double])]
        iter.foreach { case (idx, point) =>
          // calculate next index number
          val childrenCenters = Array(2 * idx, 2 * idx + 1).filter(newCenters.keySet.contains(_))
              .map(newCenters(_)).toArray
          if (childrenCenters.size >= 1) {
            val closestIndex =
              HierarchicalClustering.findClosestCenter(metric)(childrenCenters)(point)
            val nextIndex = 2 * idx + closestIndex

            // get a map value or else get a sparse vector
            val (sumBV, n, sumOfSquares) = map.get(nextIndex)
                .getOrElse(BSV.zeros[Double](point.size), 0.0, BSV.zeros[Double](point.size))
            map(nextIndex) = (sumBV + point, n + 1.0, sumOfSquares + (point :* point))
          }
        }
        map.toIterator
      }.reduceByKey { case ((sv1, n1, sumOfSquares1), (sv2, n2, sumOfSquares2)) =>
        // sum the accumulation and the count in the all partition
        (sv1 + sv2, n1 + n2, sumOfSquares1 + sumOfSquares2)
      }.collect().toMap

      // calculate the center of each cluster
      newCenters = eachStats.map { case (idx, (sum, n, sumOfSquares)) => (idx, sum :/ n)}

      // update summary of each cluster
      stats = eachStats.toMap

      variances = stats.map { case (idx, (sum, n, sumOfSquares)) =>
        math.pow(sumOfSquares.toArray.sum, 1.0 / sumOfSquares.size)
      }.sum
      diffVariances = math.abs(oldVariances - variances) / oldVariances
      log.debug(s"${sc.appName}:subIter#${subIter}, variances:${variances}, " +
          s"oldVariances:${oldVariances}, diffVariances:${diffVariances}")
      oldVariances = variances
      subIter += 1
    }
    stats
  }

  /**
   * Updates the indexes of clusters which is divided to its children indexes
   */
  private[clustering]
  def updateClusterIndex(
    data: RDD[(Int, BV[Double])],
    dividedClusters: Map[Int, ClusterTree]): RDD[(Int, BV[Double])] = {
    // extract the centers of the clusters
    val sc = data.sparkContext
    var centers = dividedClusters.map { case (idx, cluster) => (idx, cluster.center)}
    sc.broadcast(centers)

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    sc.broadcast(metric)

    // update the indexes to their children indexes
    data.map { case (idx, point) =>
      val childrenIndexes = Array(2 * idx, 2 * idx + 1).filter(centers.keySet.contains(_))
      childrenIndexes.size match {
        // stay the index if the number of children is not enough
        case s if s < 2 => (idx, point)
        // update the indexes
        case _ => {
          val nextCenters = childrenIndexes.map(centers(_)).map(_.toBreeze)
          val closestIndex = HierarchicalClustering.findClosestCenter(metric)(nextCenters)(point)
          val nextIndex = 2 * idx + closestIndex
          (nextIndex, point)
        }
      }
    }
  }
}

/**
 * A cluster as a tree node which can have its sub nodes
 *
 * @param center the center of the cluster
 * @param records the number of rows in the cluster
 * @param variances variance vectors
 * @param parent the parent cluster of the cluster
 * @param children the children nodes of the cluster
 * @param sumOfSquaresVariance the sum of squares of variances
 */
class ClusterTree(
  val center: Vector,
  val records: Long,
  val variances: Vector,
  private var localHeight: Double,
  private var parent: Option[ClusterTree],
  private var children: Array[ClusterTree],
  private var sumOfSquaresVariance: Option[Double]) extends Serializable {

  def this(center: Vector, rows: Long, variances: Vector) =
    this(center, rows, variances, 0.0, None, Array.empty[ClusterTree], None)

  /**
   * Inserts sub nodes as its children
   *
   * @param children inserted sub nodes
   */
  def insert(children: Array[ClusterTree]) {
    this.children = this.children ++ children
    children.foreach(child => child.parent = Some(this))
  }

  /**
   * Inserts a sub node as its child
   *
   * @param child inserted sub node
   */
  def insert(child: ClusterTree) {
    insert(Array(child))
  }

  /**
   * Converts the tree into Array class
   * the sub nodes are recursively expanded
   *
   * @return Array class which the cluster tree is expanded
   */
  def toArray(): Array[ClusterTree] = {
    val array = this.children.size match {
      case 0 => Array(this)
      case _ => Array(this) ++ this.children.flatMap(child => child.toArray().toIterator)
    }
    array.sortWith { case (a, b) =>
      a.getDepth() < b.getDepth() && a.variances.toArray.sum < b.variances.toArray.sum
    }
  }

  /**
   * Gets the depth of the cluster in the tree
   *
   * @return the depth
   */
  def getDepth(): Int = {
    this.parent match {
      case None => 0
      case _ => 1 + this.parent.get.getDepth()
    }
  }

  /**
   * Gets the leaves nodes in the cluster tree
   */
  def getLeavesNodes(): Array[ClusterTree] = {
    this.toArray().filter(_.isLeaf()).sortBy(_.center.toArray.sum)
  }

  def isLeaf(): Boolean = (this.children.size == 0)

  def getParent(): Option[ClusterTree] = this.parent

  def getChildren(): Array[ClusterTree] = this.children

  /**
   * Gets the sum of squares variances
   */
  def getSumOfSquaresVariances(): Double = {
    // set the result of calculation in order to reduce calculating time
    // because if the dimensions is very high such as 100000, it takes a long time to calculate it
    if (this.sumOfSquaresVariance.isEmpty) {
      val sumOfSquares = this.variances.toBreeze :* this.variances.toBreeze
      this.sumOfSquaresVariance = Some(breezeNorm(sumOfSquares, 1.0 / this.variances.size))
    }
    this.sumOfSquaresVariance.get
  }

  /**
   * Gets the dendrogram height of the cluster at the cluster tree
   *
   * @return the dendrogram height
   */
  def getHeight(): Double = {
    this.children.size match {
      case 0 => 0.0
      case _ => this.localHeight + this.children.map(_.getHeight()).max
    }
  }

  private[mllib]
  def setLocalHeight(height: Double) = (this.localHeight = height)
}
