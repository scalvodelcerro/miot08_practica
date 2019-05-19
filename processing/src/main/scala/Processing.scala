import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._
import scala.concurrent.duration._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Processing {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Uso: Loader <fichero>")
      System.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("Loader")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Read from xml
    val readTimeStart = System.nanoTime
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag","row")
      .load(args(0))
    val readTimeEnd = System.nanoTime

    // Aggregate data 
    val aggregateTimeStart = System.nanoTime
    val aggs = df
      .groupBy("_DisplayName")
      .agg(Map("_Reputation" -> "max", "_Views" -> "max"))
      .cache
    aggs.show
    val aggregateTimeEnd = System.nanoTime

    // Get top 10 users with most reputation
    val reputationTimeStart = System.nanoTime
    val topTenReputation = aggs
      .sort(desc("max(_Reputation)"))
      .select("_DisplayName", "max(_Reputation)")
      .take(10)
    println("---Top ten users by reputation")
    println(topTenReputation)
    val reputationTimeEnd = System.nanoTime

    // Get top 10 users with most views
    val viewsTimeStart = System.nanoTime
    val topTenViews = aggs
      .sort(desc("max(_Views)"))
      .select("_DisplayName", "max(_Views)")
      .take(10)
    println("---Top ten users by views")
    println(topTenViews)
    val viewsTimeEnd = System.nanoTime

    // Print stats
    println("------")
    println("--- Processing stats")
    println("")
    println(" XML read time     : " +
      Duration( readTimeEnd - readTimeStart , NANOSECONDS).toSeconds + " seconds")
    println(" Aggregation time  : " +
      Duration( aggregateTimeEnd - aggregateTimeStart , NANOSECONDS).toSeconds + " seconds")
    println(" By reputation time: " +
      Duration( reputationTimeEnd - reputationTimeStart , NANOSECONDS).toSeconds + " seconds")
    println(" By views time     : " +
      Duration( viewsTimeEnd - viewsTimeStart , NANOSECONDS).toSeconds + " seconds")
  }
}