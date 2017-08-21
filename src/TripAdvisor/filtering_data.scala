package TripAdvisor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.avro.data.Json
import org.apache.spark.sql._

object filtering_data {
  
  val base_json_path = "C:/Users/polarium/Desktop/ALS/TripAdvisorJson/json/"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Read Data")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
      
    try {
       val HotelData = spark.read.json(base_json_path+"72572.json")
         
      println("count file : "+HotelData.count())
      HotelData.printSchema()
      
      HotelData.createOrReplaceTempView("HotelData")
      val data = spark.sql(
          "select "+
            "Reviews.Ratings.Overall as Rating, "+
            "Reviews.ReviewID as userID, "+
            "HotelInfo.HotelID as itemID "+
          "from "+
            "HotelData "
      )
      
    } catch {
      case error:Exception => println(error)
    }
  }
  
}