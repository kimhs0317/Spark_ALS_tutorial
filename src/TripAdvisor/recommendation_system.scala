package TripAdvisor

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object recommendation_system {
  
  val json_FilePath = "C:/Users/polarium/Desktop/ALS/TripAdvisorJson/json/"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Recommendation System")
      .getOrCreate()
    //log level setting  
    spark.sparkContext.setLogLevel("ERROR")
    
    try {
      
    }catch {
      case error: Exception => println(error)
    }
  }
  
  private def Learn_ALS_Model(spark: SparkSession): Unit = {
    val rating = spark.read.json(json_FilePath+"72572.json")
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