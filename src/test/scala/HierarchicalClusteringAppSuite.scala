import org.scalatest.FunSuite

class HierarchicalClusteringAppSuite extends FunSuite {

  test("main") {
    val master = "local[2]"
    val cores = 2
    val rows = 1000
    val numClusters = 100
    val dimension = 10000
    val numPartitions = cores

    val args = Array(
      master,
      cores,
      rows,
      numClusters,
      dimension,
      numPartitions
    ).map(_.toString)
    BisectingKMeansApp.main(args)
  }
}
