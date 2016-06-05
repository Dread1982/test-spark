import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object SampleApp {
  def main(args: Array[String]) {
    val logFile = "/spark/README.md" // Should be some file on your system
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    val movies = sc.cassandraTable("video", "movies_by_actor")
    println(movies.first())
    println()

    val movies_trans = movies.filter(row => row.getString("title").toLowerCase.contains("pirate")).foreach(println)
    println()

  }
}