import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._

object SparkMatrix{
  def main(args: Array[String]){
    if (args.length == 0){
      System.err.println("Usage: SparkMatrix <master>")
      System.exit(1)
    }
    def dotMultiply(a:Seq[(String,Double)], b:Seq[(String,Double)]):Double={
      var result = 0.0
      if(a.length == b.length){
        for(i <- 0 to a.length-1){
          result += a(i)._2 * b(i)._2
        }
      }
      return result
    }
    val conf = new SparkConf().setAppName("SparkMatrix")
    val sc = new SparkContext(conf)
    val text1=sc.textFile(args(0))
    val text2=sc.textFile(args(1))
    val A=text1.map{x => val line=x.split(",");(line(0),(line(1),line(2).toDouble))}.groupByKey()
    val B=text2.map{x => val line=x.split(",");(line(1),(line(0),line(2).toDouble))}.groupByKey()
    val C=A.cartesian(B)
    C.collect().foreach(println)
    val D=C.map(x => ((x._1._1,x._2._1),dotMultiply(x._1._2,x._2._2)))
    val E=D.map(x => x._1._1+","+x._1._2+","+x._2.toString).saveAsTextFile(args(2))
    D.collect().foreach(println)
  }
}