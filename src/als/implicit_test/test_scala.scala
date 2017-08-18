package als.implicit_test

import scala.io.Source

object test_scala {
  
  def main(args: Array[String]): Unit = {
        
//      val str = "test"
//      val fields = str.split("\t")
      
    try{
      for(line <- Source.fromFile("data/music/artist_data.txt", "ISO-8859-1").getLines()){
        val fields = line.split("\t")
        print("singer id : "+ fields(0))
        println("\tsinger name : "+ line.replace(fields(0)+"\t",""))
      }
    } catch {
      case ex : Exception => println(ex)
    }
    
//      println("str: "+fields.map(i => i(0)).get)
//      println("str: "+fields.map(i => i(1)).get)
  }
}