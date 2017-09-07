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
      
    	kMeans(spark)
      
    } catch {
      case error:Exception => println(error)
    }
  }
  
  
  def kMeans(spark: SparkSession): Unit = {
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
//    println("count : "+df2.count())
    
    val assembler = new VectorAssembler()
        .setInputCols(Array("num_cities", "num_helpful_votes", "num_reviews", "num_type_reviews", "itemID", "helpful_votes", "offering_id"))
        .setOutputCol("feature")
        
    val df3 = assembler.transform(df2)
    
    val kmeans = new KMeans()
        .setK(5)
        .setFeaturesCol("feature")
    val model = kmeans.fit(df3)
    
    model.clusterCenters.foreach(println)
    
    val WSSSE = model.computeCost(df3)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
  }
  
}