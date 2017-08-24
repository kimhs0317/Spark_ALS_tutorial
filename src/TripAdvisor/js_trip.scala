package TripAdvisor

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object js_trip {
  case class Ratings(userId: Long, hotelId: Long, rating: Double)

  case class HotelRating(userId: Int, hotelId: Int, rating: Float)
  
// 	val JSON_PATH = "file:///C:/Users/USER/Desktop/Private/SampleData/CF/TripAdvisorJson/json/729373.json"
 	val JSON_PATH_ALL = "C:/Users/polarium/Desktop/ALS/TripAdvisorJson/json/*.json"
  
  val schema = ScalaReflection.schemaFor[Ratings].dataType.asInstanceOf[StructType]
  
  val toInt = udf[Int, String](_.toInt)
  val toDouble = udf[Double, String](_.toDouble)
  val toFloat = udf[Float, String](_.toFloat)
  val toLong = udf[Long, String](_.toLong)
  
  def main (args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ALSExample")
      .getOrCreate()
    import spark.implicits._    
      
    val hotelReviews = spark.read.json(JSON_PATH_ALL)

    val flattened = hotelReviews.select($"HotelInfo", explode($"Reviews").as("Reviews_flat"))
    val ratingDF = flattened.select(($"HotelInfo.HotelID").as("hotelId"), ($"Reviews_flat.Author").as("userStringId"), ($"Reviews_flat.Ratings.Overall").as("rating"), ($"Reviews_flat.Date").as("timestamp"))
    
    val users = ratingDF
      .select(($"userStringId"))
      .dropDuplicates()
      .withColumn("userId", monotonically_increasing_id)
        
    val ratingsDF2 = ratingDF
      .join(users, ratingDF("userStringId") === users("userStringId"), "left")
      .withColumn("rating", toDouble(ratingDF("rating")))
      .withColumn("hotelId", toInt(ratingDF("hotelId")))
    
    val ratingsDF3 = ratingsDF2.select($"userId", $"hotelId", $"rating")
    
    // Convert to DataSet from DataFrame
    val ratings = ratingsDF3.as[Ratings]
      .filter($"userId".isNotNull)
      .filter($"hotelId".isNotNull)
      .filter($"rating".isNotNull)
      
    ratings.coalesce(1).write.format("csv").save("C:/Users/polarium/Desktop/ALS/TripAdvisorJson/js/data")
//    users.coalesce(1).write.format("csv").save(base_json_path+"js/user")
  }
}