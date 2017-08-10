package scala.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object test_example2 {
  
  def main(args: Array[String]): Unit = {
    val testFile = "data/test.data"
    val conf = new SparkConf().setAppName("My First Spark App").setMaster("local[2]")
    var sc = new SparkContext(conf)
    val testData = sc.textFile(testFile, 2).cache()
    
    val rdd = sc.parallelize(List("a", "a", "b", "b")).map((_, 1))
    println("rdd : "+rdd.collect.mkString(","))
    val result = rdd.reduceByKey(_ + _)
    println("result : "+result.collect.mkString(","))

    println(testData.count())
  }
  
}