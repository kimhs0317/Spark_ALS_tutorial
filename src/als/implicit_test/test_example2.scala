package als.implicit_test

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import breeze.linalg._
import org.apache.commons.math3.analysis.function.Subtract
import org.apache.spark.ml.regression.GeneralizedLinearRegression.Power
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameReader

object test_example2 {
  
  case class Rating(userid:Int, singerid:Int, rating:Float)
  case class Singer(singerid:String, singername:String)
  val file_path = "C:/Users/polarium/Desktop/ALS/result"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("implicit_ALS")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try {
//      run_ALS(spark)
      result_sql(spark)
    } catch {
      case ex : Exception => println(ex)
    }
    
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
    val singerID = fields(0)
    val singerName = str.replace(fields(0)+"\t","")
    Singer(singerID.toString(), singerName.toString())
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
      .setAlpha(40.0)
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(1)  //lambda
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
    
//    val userRecs = model.recommendForAllUsers(3)
//    userRecs.show()
//    userRecs.coalesce(1).write.json(file_path+"/test_result")
  }
  
  //test sparkSQL
  private def result_sql(spark:SparkSession): Unit = {
    import spark.implicits._
    
    val result = spark.read.json(file_path+"/test_result/*.json")
    val singer = spark.read.textFile("data/music/artist_data.txt")
                      .map(parseSinger)
                      .toDF()
    
    result.createOrReplaceTempView("rec_song")
    singer.createOrReplaceTempView("singer")
    
    val recommendation = spark.sql("select userid, recommendations.singerid from rec_song")
    recommendation.createOrReplaceTempView("recom")
    
    val sql = 
      "select "+
        "r.userid, "+
        "s.* "+
      "from "+
        "singer s, "+
        "recom r "+
      "where "+
        "s.singerid = r.singerid[0] or "+
        "s.singerid = r.singerid[1] or "+
        "s.singerid = r.singerid[2] "+
      "order by "+
        "r.userid asc"
    val rec_singer = spark.sql(sql)
    
    rec_singer.coalesce(1).write.json(file_path+"/recommendate singer")
       
    singer.unpersist()
    result.unpersist()
    recommendation.unpersist()
  }
}