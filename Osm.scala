import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.graphx._

object Osm {

  def main(args: Array[String]): Unit = {
    // uncomment below line and change the placeholders accordingly
    val sc = SparkSession.builder().master("spark://richmond:30356").getOrCreate().sparkContext

    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
    //    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    //Args, NodeList, EdgeList, StartNode DestinationNode

    //Two Files, .nodelist and .edgelist
    //NodeList format:
    //  nodeID latitude longitude
    //EdgeList format:
    //  sourceNodeID destNodeID distance

    //Read NodeList into RDD
    var node_list_lines = sc.textFile(args(0))
    var nodes = node_list_lines.map(s=>(s.split(" ")(0)))

    //Read EdgeList into RDD
    var vertex_list_lines = sc.textFile(args(1))
    var vertecies = vertex_list_lines_list_lines.map(s=>(s.split(" ")(0)))

    //Make Nodes of graph
    val graph_nodes: RDD[(VertexId, (String, String))] = node_list_lines.map(s=>(s.split(" ")(0), (s.split(" ")(1), s.split(" "))(2))

    val graph_vertex: RDD[Edge[Double]] = vertex_list_lines.map(s=> Edge(s.split(" ")(0), s.split(" ")(1), s.split(" ")(2)))

    val defaultPlace = ""

    var sortedagain = result.reduceByKey((a,b) => a+b).sortBy(_._2, false)
    //Output!
    sortedagain.saveAsTextFile(args(2))
  }
}
