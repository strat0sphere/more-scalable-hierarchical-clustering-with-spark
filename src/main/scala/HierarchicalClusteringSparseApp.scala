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

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.uncommons.maths.random.XORShiftRNG


object HierarchicalClusteringSparseApp {

  def main(args: Array[String]) {

    val master = args(0)
    val cores = args(1).toInt
    val rows = args(2).toInt
    val numClusters = args(3).toInt
    val dimension = args(4).toInt
    val numPartitions = args(5).toInt
    val sparsity = args(6).toDouble

    val conf = new SparkConf()
        .setAppName("hierarchical clustering")
        .setMaster(master)
        .set("spark.cores.max", cores.toString)
        .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    val vectors = generateSparseVectors(numClusters, dimension, sparsity)
    val trainData = generateSparseData(sc, rows, numPartitions, vectors)
    val data = trainData.map(_._2)

    val trainStart = System.currentTimeMillis()
    val algo = new BisectingKMeans().setNumClusters(numClusters)
    val model = algo.run(data)
    val trainEnd = System.currentTimeMillis() - trainStart

    sc.broadcast(vectors)
    sc.broadcast(model)
    val distances = trainData.map { case (idx, point) =>
      val origin = vectors(idx)
      val diff = point.toArray.zip(origin.toArray).map { case (a, b) => (a - b) * (a - b)}.sum
      math.pow(diff, origin.size)
    }
    val failuars = distances.filter(_ > 10E-5).count


    println(s"====================================")
    println(s"Elapse Training Time: ${trainEnd / 1000.0} [sec]")
    println(s"cores: ${cores}")
    println(s"rows: ${data.count}")
    println(s"numClusters: ${model.getClusters.size}")
    println(s"dimension: ${model.getCenters.head.size}")
    println(s"numPartition: ${trainData.partitions.length}")
    println(s"sparsity: ${sparsity}")
    println(s"# Different Points: ${failuars}")
  }

  def generateSparseVectors(
    numCenters: Int,
    dimension: Int,
    sparsity: Double): Array[Vector] = {

    //    val random = new XORShiftRNG()
    //    (1 to numCenters).map { i =>
    //      var indexes = (0 to dimension - 1).map(i => (i, random.nextDouble()))
    //          .filter { case (i, rnd) => rnd <= sparsity}.map { case (i, rnd) => i}.toArray
    //      while (indexes.size == 0) {
    //        indexes = (0 to dimension - 1).map(i => (i, random.nextDouble()))
    //            .filter { case (i, rnd) => rnd <= sparsity}.map { case (i, rnd) => i}.toArray
    //      }
    //      val elements = indexes.map(i => 1000 * random.nextDouble())
    //      Vectors.sparse(dimension, indexes, elements)
    //    }.toArray
    val random = new XORShiftRNG()
    (1 to numCenters).map { i =>
      var indexes = (0 to dimension - 1).map(j => (j, random.nextDouble()))
          .filter { case (j, rnd) => rnd <= sparsity}.map { case (j, rnd) => j}.toArray
      while (indexes.size == 0) {
        indexes = (0 to dimension - 1).map(j => (j, random.nextDouble()))
            .filter { case (j, rnd) => rnd <= sparsity}.map { case (j, rnd) => j}.toArray
      }
      val elements = indexes.map(j => math.pow(0.8, i.toDouble))
      Vectors.sparse(dimension, indexes, elements)
    }.toArray
  }

  def generateSparseData(
    sc: SparkContext,
    rows: Int,
    numPartitions: Int,
    centers: Array[Vector]): RDD[(Int, Vector)] = {

    require(centers.size > 0, s"The number of centers is required to be more than 0.")

    val seeds = sc.parallelize(1 to rows, numPartitions)
    val random = new XORShiftRNG()
    sc.broadcast(random)
    sc.broadcast(centers)
    seeds.map { i =>
      val idx = (i % centers.size).toInt
      val origin = centers(idx).asInstanceOf[SparseVector]
      val indexes = origin.indices
      indexes.size match {
        case 0 => None
        case _ => {
          val values = origin.values.map(elm => elm + (elm * 0.0001 * random.nextGaussian()))
          Some((idx, Vectors.sparse(origin.size, indexes, values)))
        }
      }
    }.filter(_.isDefined).map(_.get)
  }
}
