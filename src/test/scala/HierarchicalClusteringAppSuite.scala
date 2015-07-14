import org.scalatest.FunSuite

class HierarchicalClusteringAppSuite extends FunSuite {

  test("main") {
    val master = "local[2]"
    val cores = 2
    val rows = 100000
    val numClusters = 9999
    val dimension = 20
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
