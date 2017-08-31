package TripAdvisor

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

object another_dataset {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Read Data")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    try {
      
    	filtering_data(spark)
      
    } catch {
      case error:Exception => println(error)
    }
  }
  
  
  def filtering_data(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val data = spark.read.json("C:/Users/polarium/Desktop/ALS/Hotel-Review dataset/review.txt/*.json")
    val city_count = data.select("author.location").groupBy("location").count()
    val df = data.select(
        $"author.num_cities", 
        $"author.num_helpful_votes", 
        $"author.num_reviews", 
        $"author.num_type_reviews", 
        $"id".as("itemID"), 
        $"num_helpful_votes".as("helpful_votes"), 
        $"offering_id").filter($"author.num_cities".isNotNull)
    
    val df2 = df.select(
        'num_cities.cast("int"),
        'num_helpful_votes.cast("int"),
        'num_reviews.cast("int"),
        'num_type_reviews.cast("int"),
        'itemID.cast("int"),
        'helpful_votes.cast("int"),
        'offering_id.cast("int")).na.fill(0)
        
    df2.show()
    println("count : "+df2.count())
    
    val assembler = new VectorAssembler()
        .setInputCols(Array("num_cities", "num_helpful_votes", "num_reviews", "num_type_reviews", "itemID", "helpful_votes", "offering_id"))
        .setOutputCol("feature")
        
    val df3 = assembler.transform(df2)
    
    val kmeans = new KMeans()
        .setFeaturesCol("feature")
    val model = kmeans.fit(df3)
    
    model.clusterCenters.foreach(println)
    
    
//    val user = data.select("author.username").dropDuplicates().withColumn("userid", monotonically_increasing_id)
//    val df = data.select($"author.username".as("username"), $"id".as("itemID"), $"ratings.overall".as("Rating"))
//      .join(user, Seq("username"))
//    val df2 = df.select('userid.cast("int"), 'itemID.cast("long"), 'Rating.cast("float"))
//      .toDF()
//    val Array(training, test) = df2.randomSplit(Array(0.8, 0.2))
//    
//    val als = new ALS()
//      .setMaxIter(10)
//      .setRank(20)
//      .setRegParam(0.01)  //lambda
//      .setUserCol("userid")
//      .setItemCol("itemID")
//      .setRatingCol("Rating")
//    val model = als.fit(training)
//    
//    model.setColdStartStrategy("drop")
//    
//    println(test.count())
//    
//    val prediction = model.transform(test)
//    
//    prediction.show()
//    
//    val evaluator = new RegressionEvaluator()
//      .setMetricName("rmse")
//      .setLabelCol("Rating")
//      .setPredictionCol("prediction")
//    val rmse = evaluator.evaluate(prediction)
//    println(s"Root-mean-square error = $rmse")
  }
  
}