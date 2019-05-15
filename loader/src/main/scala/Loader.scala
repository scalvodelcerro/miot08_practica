import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
import com.databricks.spark.xml._
/* spark-shell --conf spark.ui.port=4050 --packages com.databricks:spark-xml_2.10:0.4.1 */

object Loader {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Uso: Loader <fichero>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("Loader")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Read from xml
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag","row")
      .xml(args(0))
    // Write to csv
    df.write.csv("/data/home/csv")

    sc.stop()
  }
}