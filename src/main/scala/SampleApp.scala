import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object SampleApp {
  def main(args: Array[String]) {
    //    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    //
    //    val sc = new SparkContext(conf)
    //
    //    val movies = sc.cassandraTable("video", "movies_by_actor")
    //    println(movies.first())
    //    println()
    //
    //    val movies_trans = movies.filter(row => row.getString("title").toLowerCase.contains("pirate")).foreach(println)
    //    println()
    //
    //    case class Record(releaseYear: Int, title: String, rating: Option[Float])
    //
    //    val moviesToRecord = sc.cassandraTable("video", "movies_by_actor").select("release_year", "title", "rating").as((y:Int,t:String,r:Option[Float]) => new Record(y,t,r))
    //
    //    println(moviesToRecord.toDebugString)
    //    println()

    val conf = new SparkConf().setAppName("Character count")

    // ############################################################################################
    // ############## from eclipse

    //	  System.setProperty("hadoop.home.dir", "C:\\Users\\Manuel\\docker_spark\\"); // for winutils: only needed when running from eclipse
    //    val logFile = "C:\\Users\\Manuel\\Desktop\\Mails.txt" // Should be some file on your system
    //    conf.setMaster("spark://192.168.0.16:7077")
    ////    conf.set("spark.driver.host","192.168.99.100")
    // //   conf.set("spark.driver.host","localhost")
    //    conf.set("spark.driver.host","192.168.0.16")
    //    conf.setJars(List("C:\\Users\\Manuel\\Projects\\TestSpark\\target\\scala-2.10\\testspark_2.10-1.0.jar"))
    //    //conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")

    // ############################################################################################

    val logFile = "/opt/spark/README.md" // Should be some file on your system

    conf.set("spark.driver.port", "7001")
    conf.set("spark.fileserver.port", "7003")
    conf.set("spark.broadcast.port", "7004")
    conf.set("spark.replClassServer.port", "7005")
    conf.set("spark.blockManager.port", "7006")
    conf.set("spark.executor.port", "7007")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    val numCs = logData.filter(line => line.contains("c")).count()

    println("\n################################################\n")
    println("Lines with a: %s, Lines with b: %s, Lines with c: %s\n".format(numAs, numBs, numCs))
    println("################################################\n")

    sc.stop()
  }
}