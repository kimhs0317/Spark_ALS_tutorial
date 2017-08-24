package TripAdvisor

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json.JSON
import com.google.gson.JsonObject
import org.apache.spark.sql.DataFrameReader
import org.json4s.jackson.Json
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object recommendation_system {
  
  case class Rate(userID: Long, ItemID: Int, Rating: Float)
  val json_FilePath = "C:/Users/polarium/Desktop/ALS/TripAdvisorJson/"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Recommendation System")
      .getOrCreate()
    
    //log level setting  
    spark.sparkContext.setLogLevel("ERROR")
    val schema = ScalaReflection.schemaFor[Rate].dataType.asInstanceOf[StructType]
    
    try {
      Learn_ALS_Model(spark)
    }catch {
      case error: Exception => println(error)
    }
  }
  def parseSinger(userID: String, itemID: String, Rating: String): Rate = {
    Rate(userID.toInt, itemID.toInt, Rating.toInt)
  }
  
  private def Learn_ALS_Model(spark: SparkSession): Unit = {
    import spark.implicits._
    val rating = spark.read.json(json_FilePath+"test/test_result/*.json")
      .select('userID.cast("int"), 'itemID.cast("int"), 'Rating.cast("float"))
      .toDF()
          
    val Array(training, test) = rating.randomSplit(Array(0.8, 0.2))
    
    val start = System.currentTimeMillis()
    val als = new ALS()
      .setMaxIter(10)
      .setRank(30)
      .setRegParam(0.01)  //lambda
      .setUserCol("userID")
      .setItemCol("itemID")
      .setRatingCol("Rating")
    val model = als.fit(training)
    
    model.setColdStartStrategy("drop")
    
    val prediction = model.transform(test)
    
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("Rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(prediction)
    println(s"Root-mean-square error = $rmse")
    val end = System.currentTimeMillis()
    println("runtime : "+(end-start)/1000.0)
  }
  
}