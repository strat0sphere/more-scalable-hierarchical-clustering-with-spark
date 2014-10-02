import org.apache.spark.util.LocalSparkContext
import org.scalatest.FunSuite

class HierarchicalClusteringAppSuite extends FunSuite with LocalSparkContext {

  test("main") {
    val master = "local[2]"
    val rows = 10000
    val cores = 4
    val numClusters = 10
    val numPartitions = 4
    val args = Array(master, rows, cores, numClusters, numPartitions).map(_.toString)
    HierarchicalClusteringApp.main(args)
  }
}
