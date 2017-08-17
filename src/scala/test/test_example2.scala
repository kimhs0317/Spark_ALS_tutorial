package scala.test

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrameNaFunctions
import scala.collection.mutable.WrappedArray

object test_example2 {
  
  case class Rating(userid:Int, singerid:Int, rating:Float)
  case class Singer(singerid:Int, singername:String)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("implicit_ALS")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
//    run_ALS(spark)
    result_sql(spark)
    
    spark.stop()
  }
  
  //Ratings set
  def parseRating(str: String): Rating = {
    val fields = str.split(" ")
    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }
  def parseSinger(str: String): Singer = {
    val fields = str.split("\t")
    Singer(fields(0).toInt, fields(1).toString())
  }
  
  //ALS setting
  private def run_ALS(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val ratings = spark.read.textFile("data/music/user_artist_data.txt")
      .map(parseRating)
      .toDF()
      
    val Array(real, trash) = ratings.randomSplit(Array(0.0001, 0.9999))
    val Array(training, test) = real.randomSplit(Array(0.8, 0.2))
    
    val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userid")
      .setItemCol("singerid")
      .setRatingCol("rating")
      .setImplicitPrefs(true)
    val model = als.fit(training)

    model.setColdStartStrategy("drop")
    
    val predictions = model.transform(test)
    
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions) 
    
    println(s"Root-mean-square error = $rmse")
    
    val userRecs = model.recommendForAllUsers(3)
    
//    userRecs.coalesce(1).write.json("data/ex_result")
//    userRecs.show(10)
  }
  
  //test sparkSQL
  private def result_sql(spark:SparkSession): Unit = {
    import spark.implicits._
    
    val result = spark.read.json("data/ex_result/part-00000-3f15fe20-e84a-4b68-9736-c47c14a6b9d7-c000.json")
    val singer = spark.read.textFile("data/music/artist_data.txt")
                      .map(parseSinger)
                      .toDF()
    
    result.createOrReplaceTempView("rec_song")
    singer.createOrReplaceTempView("singer")
    
    val recommendation = spark.sql("select recommendations.singerid as singerid from rec_song where userid = 2008050")
    recommendation.createOrReplaceTempView("recom")
    
    val rec_singer = spark.sql("select * from singer s, recom r where s.singerid = r.singerid[0]")
    
    rec_singer.show()
       
    singer.unpersist()
    result.unpersist()
  }
}