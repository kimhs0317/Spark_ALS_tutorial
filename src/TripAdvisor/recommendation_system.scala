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
  
  case class Rate(userID: Int, ItemID: Int, Rating: Float)
  val json_FilePath = "C:/Users/polarium/Desktop/ALS/TripAdvisorJson/"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Recommendation System")
      .getOrCreate()
    import spark.implicits._
    //log level setting  
    spark.sparkContext.setLogLevel("ERROR")
    val schema = ScalaReflection.schemaFor[Rate].dataType.asInstanceOf[StructType]
    
    try {
      val data = spark.read.schema(schema).json(json_FilePath+"test/test_result/*.json").as[Rate]
      data.filter($"userID".isNotNull).show()
      
    }catch {
      case error: Exception => println(error)
    }
  }
  
  private def Learn_ALS_Model(spark: SparkSession): Unit = {
    val rating = spark.read.json(json_FilePath+"test_result/*.json")
      .toDF()
      
    val Array(training, test) = rating.randomSplit(Array(0.8, 0.2))
    
    val als = new ALS()
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(0.1)  //lambda
      .setUserCol("userID")
      .setItemCol("hotelID")
      .setRatingCol("overall")
    val model = als.fit(training)
    
    model.setColdStartStrategy("drop")
    
    val prediction = model.transform(test)
    
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(prediction)
    println(s"Root-mean-square error = $rmse")
  }
  
}