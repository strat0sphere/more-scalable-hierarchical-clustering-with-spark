import org.scalatest.FunSuite

class HierarchicalClusteringAppSuite extends FunSuite {

  test("main") {
    val master = "local[2]"
    val rows = 10000
    val cores = 2
    val numClusters = 10
    val dimension = 10
    val numPartitions = cores

    val args = Array(
      master,
      rows,
      cores,
      numClusters,
      dimension,
      numPartitions
    ).map(_.toString)
    val model = HierarchicalClusteringApp.main(args)
  }
}
