/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

object SimpleApp {
  def main(args: Array[String]) {

    val sc = new SparkContext("spark://albus.suse:7077", "Simple Application")
    // Carga os vertices em tuplas page id e uma lista de atributos
    val users = (sc.textFile("/home/marcos/workspace/graphx/data/pt_wiki/nodes/x_nodes.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))
    // Carga as arestas em tuplas pageIdFonte pageIdObjetivo Link
    val followerGraph = GraphLoader.edgeListFile(sc, "/home/marcos/workspace/graphx/data/pt_wiki/edges/x_edges.txt")
    // Cria o grafo
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList      
      case (uid, deg, None) => Array.empty[String]
    }
    // Calcula o pageRank
    val pagerankGraph = graph.staticPageRank(30)
    // Obtem a informacao das paginas
    val userInfoWithPageRank = graph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }
    // Imprime o resultado
    println(userInfoWithPageRank.vertices.top(20)(Ordering.by(_._2._1)).mkString("\n"))    
    println(" > the end > ")
  }
}
