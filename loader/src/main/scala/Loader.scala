import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
import com.databricks.spark.xml._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Loader {
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
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag","row")
      .load(args(0))

    // Write to csv
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .mode("overwrite")
      .save("/scalvo/stackoverflow/csv")
  }
}