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
    
    try {
//      Learn_ALS_Model(spark)
      recommendate(spark)
    }catch {
      case error: Exception => println(error)
    }
  }
  
  private def Learn_ALS_Model(spark: SparkSession): Unit = {
    import spark.implicits._
    val rating = spark.read.json(json_FilePath+"Colaborative Filtering BaseData/*.json")
      .select('userID.cast("int"), 'itemID.cast("int"), 'Rating.cast("float"))
      .toDF()
          
    val Array(training, test) = rating.randomSplit(Array(0.8, 0.2))
    
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
    
    val user = model.recommendForAllUsers(5)
    
//    user.coalesce(1).write.json(json_FilePath+"recommendForAllUsers")
  }
  
  private def recommendate(spark: SparkSession): Unit = {
     import spark.implicits._
     val result = spark.read.json(json_FilePath+"recommendForAllUsers/*.json")
     val user = spark.read.json(json_FilePath+"UserList/*.json")
     
//     val recommend = result.select($"userID", explode($"recommendations").as("recommend"))
//       .withColumn("itemID", $"recommend.itemID")
     val recommend = result.select("userID", "recommendations.itemID")
       .join(user, Seq("userID"))
     recommend.show()
     
//     user.createOrReplaceTempView("user")
//     recommend.createOrReplaceTempView("recommend")
//     val re_list = spark.sql(
//        "select "+
//          "u.Author as user, "+
//          "r.itemID as recommendItem "+
//        "from "+
//          "recommend r "+
//          "LEFT JOIN user u on r.userID = u.userID "+
//        "ORDER BY r.userID ASC"
//    )
//    
//    re_list.show()
  }
}