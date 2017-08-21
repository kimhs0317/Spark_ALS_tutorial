package als.implicit_test

import scala.io.Source
import java.io.File

object test_scala {
  
  def main(args: Array[String]): Unit = {
        
//      val str = "test"
//      val fields = str.split("\t")
      
    try{
      for(line <- Source.fromFile("data/music/artist_data.txt", "ISO-8859-1").getLines()){
        val fields = line.split("\t")
        
        if(fields(0).matches("[a-zA-Z]*")){
          println("fields :"+fields(0).toString())
        }
        
//        print("singer id : "+ fields(0))
//        println("\tsinger name : "+ line.replace(fields(0)+"\t",""))
      }
    } catch {
      case ex : Exception => println(ex)
    }
  }
}