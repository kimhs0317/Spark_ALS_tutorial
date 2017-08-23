package TripAdvisor

import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object filtering_data {
  
  val base_json_path = "C:/Users/polarium/Desktop/ALS/TripAdvisorJson/"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Read Data")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
      
    try {
      val start = System.currentTimeMillis()
      
      save_file(spark)
//      read_file(spark)
      
      val end = System.currentTimeMillis()
      println("runtime : "+(end-start)/1000.0)
      
    } catch {
      case error:Exception => println(error)
    }
  }
  
  def save_file(spark:SparkSession): Unit = {
    val HotelData = spark.read.json(base_json_path+"json/7*.json")
    
    //HotelData view 생성 후 query로 필요한 데이터를 뽑아온다.
    HotelData.createOrReplaceTempView("HotelData")
    val data = spark.sql(
        "select "+
          "AuthorID, "+
          "Author, "+
          "HotelInfo.HotelID as itemID, "+
          "Rating "+
        "from "+
          "HotelData "+
        "LATERAL VIEW posexplode(Reviews.Author) a AS AuthorID, Author "+
        "LATERAL VIEW posexplode(Reviews.Ratings.Overall) o AS RatingID, Rating "+
        "where "+
          "AuthorID = RatingID"
    )
    data.show()
    
    data.coalesce(1).write.json(base_json_path+"test_result")
//    base_data.coalesce(1).write.json(base_json_path+"Colaborative Filtering BaseData")
  }
  
  def read_file(spark:SparkSession): Unit = {
    val sample = spark.read.json(base_json_path+"test_result/*.json")
    sample.printSchema()
    sample.show()
  }
}