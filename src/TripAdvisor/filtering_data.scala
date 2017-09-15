package TripAdvisor

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object filtering_data {
  
  val base_json_path = "C:/Users/polarium/Desktop/ALS/TripAdvisorJson/"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Read Data")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    try {
      //17minutes..
//      save_file(spark)
      reload_file(spark)
    } catch {
      case error:Exception => println(error)
    }
  }
  
  def save_file(spark:SparkSession): Unit = {
    import spark.implicits._
    val HotelData = spark.read.json(base_json_path+"json/*.json")
    
    HotelData.printSchema()
    
    //HotelData view 생성 후 query로 필요한 데이터를 뽑아온다.
    HotelData.createOrReplaceTempView("HotelData")
    val seq = spark.sql(
        "select "+
          "ROW_NUMBER() OVER(ORDER BY Author) as userID, "+
          "Author "+
        "from "+
          "HotelData "+
        "LATERAL VIEW explode(Reviews.Author) a AS Author "+
        "GROUP BY Author"
    )
    val base_data = HotelData.select(explode($"Reviews").as("Reviews"), $"HotelInfo.HotelID".as("itemID"))
      .withColumn("Author", $"Reviews.Author")
      .withColumn("Rating", $"Reviews.Ratings.Overall")
    val data = base_data.join(seq, Seq("Author")).select("userID", "itemID", "Rating")
//    data.coalesce(1).write.json(base_json_path+"Colaborative Filtering BaseData")
  }
  
  def reload_file(spark:SparkSession): Unit = {
    val sample = spark.read.json(base_json_path+"recommendForAllUsers/*.json")
    sample.printSchema()
    sample.show()
  }
}