/*
 * Programming Assignment 1 on Big Data Ecosystem
 * Fuyong Xing
 * This code is written by following the class slides and the Spark tutorial at http://www.apache.org
 * Usage: BigDataPageRankGX <input file> <output path of ranks> <output path of total running time> <number of iteration> <number of top ranks>
 */


import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.xml.{NodeSeq, XML}


object BigDataPageRankGX {
  def main(args: Array[String]) {

    // spark configuration
    val totalBeginTime = System.currentTimeMillis()
    val sparkConf = new SparkConf()
    sparkConf.setAppName("BigDataPageRankGX")
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile(args(0))

    // parse data
    var verticesedge = input.map(line => {
      val parts = line.split("\t")
      val (articleID, content) = (parts(1), parts(3).replace("\\n", "\n"))
      val outlinks =
        if (content == "\\N") { NodeSeq.Empty }
        else {
          try { XML.loadString(content) \\ "link" \ "target" }
          catch { case e: org.xml.sax.SAXParseException => NodeSeq.Empty }
        }
      val adjEdges = outlinks.map(link => new String(link.text)).toArray
      val verticeID = new String(articleID)
      (verticeID, adjEdges)
    })

    // make vertice and edge RDD
    def pageHash(title: String): VertexId = {
      title.toLowerCase.replace(" ", "").hashCode.toLong
    }
    val vertices = verticesedge.map(a => (pageHash(a._1), a._1)).cache
    val edges: RDD[Edge[Double]] = verticesedge.flatMap { a =>
      val srcID = pageHash(a._1)
      a._2.map {b =>
        val dstID = pageHash(b)
        Edge(srcID, dstID, 1.0)
      }
    }

    // build graph and page rank
    val graph = Graph(vertices, edges, "").subgraph(vpred = {(v, d) => d.nonEmpty}).cache
    val prGraph = graph.staticPageRank(args(3).toInt).cache
    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    // print and write top args(4) ranks
    val totalEndTime = System.currentTimeMillis()
    val totalTime = (totalEndTime - totalBeginTime)/1000.0
    sc.makeRDD(titleAndPrGraph.vertices.top(args(4).toInt) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.map(t => (t._2._2, t._2._1))).saveAsTextFile(args(1))
    sc.makeRDD(List(totalTime)).saveAsTextFile(args(2))
    titleAndPrGraph.vertices.top(args(4).toInt) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))
    println("Total running time (GraphX): " + totalTime + " seconds.")
    sc.stop()
  }
}

