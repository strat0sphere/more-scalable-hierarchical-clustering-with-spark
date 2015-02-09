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
import org.apache.spark.Logging
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * This class is used for the model of the hierarchical clustering
 *
 * @param tree a cluster as a tree node
 */
class HierarchicalClusteringModel(val tree: ClusterTree)
    extends Serializable with Logging {

  def getClusters(): Seq[ClusterTree] = this.tree.getLeavesNodes()

  def getCenters(): Seq[Vector] = this.getClusters().map(_.center)

  /**
   * Predicts the closest cluster by one point
   */
  def predict(vector: Vector): Int = {
    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)

    val centers = this.getCenters().map(_.toBreeze)
    HierarchicalClustering.findClosestCenter(metric)(centers)(vector.toBreeze)
  }

  /**
   * Predicts the closest cluster by RDD of the points
   */
  def predict(data: RDD[Vector]): RDD[Int] = {
    val sc = data.sparkContext

    // TODO Supports distance metrics other Euclidean distance metric
    val metric = (bv1: BV[Double], bv2: BV[Double]) => breezeNorm(bv1 - bv2, 2.0)
    sc.broadcast(metric)
    val centers = this.getCenters().map(_.toBreeze)
    sc.broadcast(centers)

    data.map{point =>
      HierarchicalClustering.findClosestCenter(metric)(centers)(point.toBreeze)
    }
  }

  /**
   * Predicts the closest cluster by RDD of the points for Java
   */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]
}

