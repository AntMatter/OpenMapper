import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.graphframes._
import org.apache.spark.sql.Row
import org.apache.spark.graphx.EdgeDirection

object Osm {


//Used from https://livebook.manning.com/book/spark-graphx-in-action/chapter-6/31
def dijkstra[VD](g:Graph[VD,Double], origin:VertexId) = {
  var g2 = g.mapVertices(
    (vid,vd) => (false, if (vid == origin) 0 else Double.MaxValue,
                 List[VertexId]()))

  for (i <- 1L to g.vertices.count-1) {
    val currentVertexId =
      g2.vertices.filter(!_._2._1)
        .fold((0L,(false,Double.MaxValue,List[VertexId]())))((a,b) =>
           if (a._2._2 < b._2._2) a else b)
        ._1

    val newDistances = g2.aggregateMessages[(Double,List[VertexId])](
        ctx => if (ctx.srcId == currentVertexId)
                 ctx.sendToDst((ctx.srcAttr._2 + ctx.attr,
                                ctx.srcAttr._3 :+ ctx.srcId)),
        (a,b) => if (a._1 < b._1) a else b)

    g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
      val newSumVal =
        newSum.getOrElse((Double.MaxValue,List[VertexId]()))
      (vd._1 || vid == currentVertexId,
       math.min(vd._2, newSumVal._1),
       if (vd._2 < newSumVal._1) vd._3 else newSumVal._2)})
  }

    g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
    (vd, dist.getOrElse((false,Double.MaxValue,List[VertexId]()))
             .productIterator.toList.tail))
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
    var nodes = node_list_lines.map(s=>(s.split(" ")(0), (s.split(" ")(1), s.split(" ")(2))))

    //Read EdgeList into RDD
    var edge_list_lines = sc.textFile(args(1))
    var edges = edge_list_lines.map(s=>(s.split(" ")(0), (s.split(" ")(1), s.split(" ")(2))))

    val nodesRdd : RDD[(VertexId, Any)] = node_list_lines.map(s =>(s.split(" ")(0).toLong, (s.split(" ")(0) + ": " + s.split(" ")(1) + " " + s.split(" ")(2))))

    val edgesRDD = edge_list_lines.map(s =>(Edge(s.split(" ")(0).toLong, s.split(" ")(1).toLong, s.split(" ")(2).toDouble)))
    val reversedEdgeRdd = edge_list_lines.map(s =>(Edge(s.split(" ")(1).toLong, s.split(" ")(0).toLong, s.split(" ")(2).toDouble)))

    val combinedEdges = edgesRDD.union(reversedEdgeRdd)

    val myGraph = Graph(nodesRdd, combinedEdges)

    val nodeiD = nodesRdd.map(_._1)

    nodeiD.collect().foreach(s=> {
      //nodeiD.foreach(s=> {
      val path = dijkstra(myGraph, s).vertices.map(_._2).collect
      val pathConvert = path.asInstanceOf[Array[(String, List[(Double, List[VertexId])])]]

      //This line might probably break everything...
      val formatted = pathConvert.map(s => s._1 + " " + s._2.toString.replaceAll("""[List()\[\]]""", "").replaceAll(",", ""))
      val out = sc.parallelize(formatted)
      
      val realout = out.coalesce(1).saveAsTextFile(args(2) + "/" + s.toString)
    })

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
