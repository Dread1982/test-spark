import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object SampleApp {
  def main(args: Array[String]) {
    val logFile = "/spark/README.md" // Should be some file on your system
  //  val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    
    val sc = new SparkContext(conf)
    
    val test_spark_rdd = sc.cassandraTable("test", "my_table")
    println(test_spark_rdd.first)
    
    val movies = sc.cassandraTable("video", "movies_by_actor")
    println(movies.first())
    println()
    
    val movies_trans = movies.filter(row => row.getString("title").toLowerCase.contains("pirate")).foreach(println)
    println()
    
    
//    
//    val logData = sc.textFile(logFile, 2).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    val numCs = logData.filter(line => line.contains("c")).count()
//    println("Lines with a: %s, Lines with b: %s, Lines with c: %s\n".format(numAs, numBs, numCs))
    
  }
}