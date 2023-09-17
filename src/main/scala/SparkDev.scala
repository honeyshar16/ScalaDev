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
    val df = List((85, "Maths"), (90, "Science"), (95, "Comp"), (45, "Social"), (70, "English")).toDF("Score", "Subject")


    df.select(col("Score"), col("Subject"),
      when(col("Score") >= 90, "A")
      .when(col("Score") >= 70  && col("Score") <=89, "B")
        .otherwise("C").as("Grade")).show()
  }
}
