import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkDev {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession

    val spark = SparkSession.builder.appName("WhenAndOther").master("local[4]").getOrCreate()

    import spark.implicits._
    val df = List((25, "male"), (18, "female"), (10, "male"), (45, "female"), (67, "female")).toDF("age", "gender")


    df.select(col("gender"), col("age"),
      when(col("age") >= 18, "TRUE")
        .otherwise("False").as("is_adult")).show()
  }
}
