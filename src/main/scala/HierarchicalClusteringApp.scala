import org.apache.spark.mllib.clustering.HierarchicalClustering
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object HierarchicalClusteringApp {

  def main(args: Array[String]) {

    val master = args(0)
    val rows = args(1).toLong
    val cores = args(2)
    val numClusters = args(3).toInt
    val numPartitions = args(4).toInt

    val conf = new SparkConf()
        .setAppName("hierarchical clustering")
        .setMaster(master)
        .set("spark.cores.max", cores)
    val sc = new SparkContext(conf)

    val data = generateData(sc, rows)
    data.repartition(numPartitions)
    val model = HierarchicalClustering.train(data, numClusters)
  }


  def generateData(sc: SparkContext, rows: Long): RDD[Vector] = {

    def multiply(data: RDD[Vector], num: Int): RDD[Vector] = {
      num match {
        case 1 => data.union(data)
        case _ => data.union(multiply(data, num - 1))
      }
    }

    if (rows <= 1000000) {
      sc.parallelize((1 to rows.toInt).map(i => Vectors.dense(Math.random(), Math.random())), 1)
    }
    else {
      val splits = 100
      val dataSeed = sc.parallelize((1 to (rows/splits).toInt)
          .map(i => Vectors.dense(Math.random(), Math.random())), 1)
      multiply(dataSeed, splits)
    }
  }
}
