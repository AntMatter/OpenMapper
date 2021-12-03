import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.graphframes._
import org.apache.spark.sql.Row
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.util.GraphGenerators

object OsmRadial {


  //Algorithm Adapted from https://github.com/artem0/spark-graphx under GNU General Public License v3.0
  def shortestPath[VT](graph: Graph[VT,Double], sourceId: VertexId) = {
      val initialGraph : Graph[(Double, List[VertexId]), Double] = 
      graph.mapVertices((id, _) => 
      if (id == sourceId) (0.0, List[VertexId](sourceId)) else (Double.PositiveInfinity, List[VertexId]()))

      //initalize pregal with distance and a list of visited nodes as it's message, these are the pregal config parameters
      val singlePointShortestPathPregel = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out) (

        //define functions for receiving messages (vprog)
        //if message contains a smaller distance, take that over the old distance
        (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, 

        //Define functions for computing messages (Send Message)
        //Accumulates Distances
        triplet => {
          if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
          } else {
            Iterator.empty
          }
        },

        //Combine Messages (Merge)
        //Return the message with the shorter edge
        (a, b) => if (a._1 < b._1) a else b)

    singlePointShortestPathPregel.vertices
  }

  def main(args: Array[String]): Unit = {
    // uncomment below line and change the placeholders accordingly
    val spark = SparkSession.builder().master("spark://richmond:30357").getOrCreate()
    val sc = spark.sparkContext

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
    //var nodes = node_list_lines.map(s=>(s.split(" ")(0), (s.split(" ")(1), s.split(" ")(2))))

    //Read EdgeList into RDD
    var edge_list_lines = sc.textFile(args(1))
    //var edges = edge_list_lines.map(s=>(s.split(" ")(0), (s.split(" ")(1), s.split(" ")(2))))

    val nodesRdd : RDD[(VertexId, Any)] = node_list_lines.map(s =>(s.split(" ")(0).toLong, (s.split(" ")(0) + ": " + s.split(" ")(1) + " " + s.split(" ")(2))))

    val edgesRDD = edge_list_lines.map(s =>(Edge(s.split(" ")(0).toLong, s.split(" ")(1).toLong, s.split(" ")(2).toDouble)))
    val reversedEdgeRdd = edge_list_lines.map(s =>(Edge(s.split(" ")(1).toLong, s.split(" ")(0).toLong, s.split(" ")(2).toDouble)))

    val combinedEdges = edgesRDD.union(reversedEdgeRdd)

    val myGraph = Graph(nodesRdd, combinedEdges)

    val nodeiD = nodesRdd.map(_._1)

    //nodeiD.collect().foreach(s=> {
      //nodeiD.foreach(s=> {
      val path = shortestPath(myGraph, args(3).toLong).collect.mkString("\n").replaceAll("""[List()\[\]]""", "").replaceAll(" ", "").replaceAll(",", " ")

      val filteredPaths = path.split("\n").filter(s => s.split(" ")(1).toDouble < args(4).toDouble).mkString("\n")
      //val singlePath = path.split("\n").filter(s => s.split(" ")(0) == args(4))(0)
      //val pathConvert = path.asInstanceOf[Array[(String, List[(Double, List[VertexId])])]]

      //This line might probably break everything...
     // val formatted = pathConvert.map(s => s._1 + " " + s._2.toString.replaceAll("""[List()\[\]]""", "").replaceAll(",", ""))
      //val out = sc.parallelize(path)

      // FileWriter
    //val file = new File(args(2) + "/" + args(3).toString + ".results")
    val file = new File(args(2) + "/RESULT" + ".results")
    //val file2 = new File(args(2) + "/" + args(3).toString + "to" + args(4).toString  + ".results")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(filteredPaths)
    bw.close()
      
      //val realout = path.saveAsTextFile(args(2) + "/" + 176812346L.toString)
    //})

    // val path = dijkstra(myGraph, 330098703L).vertices.map(_._2).collect

    // val pathConvert = path.asInstanceOf[Array[(String, List[(Double, List[VertexId])])]]

    // val out = sc.parallelize(pathConvert)

    
    // val realout = out.coalesce(1).saveAsTextFile(args(2))

    //val rddNode = spark.createDataFrame(nodes).toDF("id", "lat")

   // val rddEdge = spark.createDataFrame(edges).toDF("id", "src", "dst")

    //rddEdge.write.csv("auto")


//     val vertex = spark.read.options(Map("delimiter"->" ")).option("header","false").csv(args(0))
//     val newNamesv = Seq("id", "lat", "lng")
//     val edgess = spark.read.options(Map("delimiter"->" ")).option("header","fal11se").csv(args(1))
//     val newNamese = Seq("src", "dst", "wgt")

//     val dfver = vertex.toDF(newNamesv: _*)
//     val dfved = vertex.toDF(newNamese: _*)

//     val graph = GraphFrame(dfver, dfved)

//     val graphx: Graph[Row, Row] = graph.toGraphX

//     //val out = graphx.vertices.saveAsObjectFile(args(2))

//     val direction: EdgeDirection = EdgeDirection.Out

//     val neighbors = graphx.collectNeighborIds(direction).lookup(5L)

//     val pwText = new PrintWriter(
//     new File( "emailMsgValues.txt" )
// )
    
//     val out = graphx.vertices.foreach(value => {pwText.write(value + "\n")})
//     pwText.close()

//     val out2 = graphx.connectedComponents.vertices.map(_.swap).groupByKey.saveAsTextFile(args(2))

    //val out = graphx.edges.filter(e => e.srcId < e.dstId).count.coalesce(1).saveAsTextFile(args(2))

    //graph.edges.write.csv(args(2))

    //Output!
    //val out = graph.edges.rdd.map(_.toString()).saveAsTextFile(args(2))
    
  }
}