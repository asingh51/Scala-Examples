
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph{
    def main(args: Array[String]){
      val conf = new SparkConf().setAppName("Graph")
      val sc = new SparkContext(conf)
      var graph = sc.textFile(args(0)).map( line => { val vector = line.split(",")//.map(_.toLong)
      (vector(0).toLong,vector(0).toLong,vector.drop(1).toList.map(_.toString).map(_.toLong)) })  
      var localGraph = graph.map(g => (g._1,g))
      localGraph.foreach(println)
      for(i <- 1 to 5){
         graph = graph.flatMap(
         map => map match{ case (x, y, xy) => (x, y) :: xy.map(z => (z, y) ) } )
        .reduceByKey((a, b) => (if (a >= b) b else a))
        .join(localGraph).map(local => (local._2._2._2, local._2._1, local._2._2._3))
    }
 val groupCount = graph.map(local => (local._2, 1))
 .reduceByKey((p, q) => (p + q))
 .collect()
 .foreach(println)
 }
}