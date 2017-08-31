package als.implicit_test

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RankingEvaluator
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameReader

object test_implicit {
  
  case class Rating(userId:Int, itemId:Int, played:Float)
  val file_path = "C:/Users/polarium/Desktop/ALS/result"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("implicit_ALS")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try {
      run_ALS(spark)
      
    } catch {
      case ex : Exception => println(ex)
    }
  }
  
  def parseRating(str: String): Rating = {
    val fields = str.split(" ")
    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }
  
  def run_ALS(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val ratings = spark.read.textFile("data/music/user_artist_data.txt")
      .map(parseRating)
      .toDF()
      
    val Array(real, trash) = ratings.randomSplit(Array(0.0001, 0.9999))
    val Array(training, test) = real.randomSplit(Array(0.8, 0.2))
    
    val als = new ALS() //input dataframe (userId, itemId, clicked)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("played")
      .setImplicitPrefs(true)

    val paramGrid = new ParamGridBuilder()
        .addGrid(als.regParam, Array(0.01,0.1))
        .addGrid(als.alpha, Array(40.0, 1.0))
        .build()
    
    val evaluator = new RankingEvaluator()
        .setMetricName("mpr") //Mean Percentile Rank
        .setLabelCol("itemId")
        .setPredictionCol("prediction")
        .setQueryCol("userId")
        .setK(5) //Top K
     
    val cv = new CrossValidator()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
    
    val crossValidatorModel = cv.fit(training)
    
    // Print the average metrics per ParamGrid entry
    val avgMetricsParamGrid = crossValidatorModel.avgMetrics
    
    // Combine with paramGrid to see how they affect the overall metrics
    val combined = paramGrid.zip(avgMetricsParamGrid)
    println(combined)
  }
}