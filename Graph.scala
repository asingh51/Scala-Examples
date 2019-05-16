import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._
object Graph {
def main(args:Array[String])
{
 val conf = new SparkConf().setAppName("Assignment8GraphX")
 val sc = new SparkContext(conf)
 val edges: RDD[Edge[Long]] = sc.textFile(args(0)).map( line => { val (vertex, rest) = line.split(",").splitAt(1)
												(vertex(0).toLong,rest.toList.map(_.toLong)) } )
												.flatMap( x => x._2.map(y => (x._1, y)))
												.map(nodes => Edge(nodes._1, nodes._2, nodes._1))
 val graph : org.apache.spark.graphx.Graph[Long, Long] = org.apache.spark.graphx.Graph.fromEdges(edges, "defaultProperty")
											.mapVertices((id, _) => id)
 val pregeloutput = graph.pregel(Long.MaxValue, 5)(                                      // 5 iterations
 (id, dist, newDist) => math.min(dist, newDist),                           
 triplet => {                                                                 
        if (triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.attr))
        } else if (triplet.srcAttr < triplet.attr) {
          Iterator((triplet.dstId, triplet.srcAttr))
        } else {
			    Iterator.empty
		    }
    },
    (a, b) => math.min(a, b)                                                
    )
pregeloutput.vertices.map(graph => (graph._2, 1)).reduceByKey(_ + _).collect().map( f => f._1.toString+" "+f._2.toString ).foreach(println)
							
    
}
}