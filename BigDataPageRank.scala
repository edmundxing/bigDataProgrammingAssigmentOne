/*
 * Programming Assignment 1 on Big Data Ecosystem
 * Fuyong Xing
 * This code is written by following the class slides and the Spark tutorial at http://www.apache.org
 * Usage: BigDataPageRank <input file> <output path of ranks> <output path of total running time> <number of iteration> <number of top ranks>
 */


import org.apache.spark.SparkContext._
import org.apache.spark._
//import org.apache.spark.graphx._
import scala.xml.{NodeSeq, XML}
import scala.collection.mutable.ListBuffer

object BigDataPageRank {
  def main(args: Array[String]) {
    val totalBeginTime = System.currentTimeMillis()
    val sparkConf = new SparkConf()
    sparkConf.setAppName("BigDataPageRank")

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

    // page rank
    var runtime = new ListBuffer[(Int,Double)]()
    verticesedge.distinct().groupByKey().cache()
    var ranks = verticesedge.mapValues(v => 1.0)
    val beginTime = System.currentTimeMillis()
    for (i <- 1 to args(3).toInt) {
      val contribs = verticesedge.join(ranks).values.flatMap{ case (urls, rank) =>
        urls.map(dest => (dest, rank / urls.size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      val endTime = System.currentTimeMillis()
      val itertime = (i, (endTime - beginTime)/1000.0)
      runtime += itertime
    }
    val runtimeList = runtime.toList

    // print and write top args(4) ranks
    val results = ranks.map(t => (-t._2,t._1)).sortByKey().take(args(4).toInt)
    val totalEndTime = System.currentTimeMillis()
    val totalTime = (totalEndTime - totalBeginTime)/1000.0
    sc.makeRDD(results.map(t => (t._2,-t._1))).saveAsTextFile(args(1))
    sc.makeRDD(List(totalTime)).saveAsTextFile(args(2))
    results.foreach(nd => println("(" + nd._2 + "," + -nd._1 + ")"))
    println("Total running time: " + totalTime + " seconds.")
    sc.stop()
  }
}
