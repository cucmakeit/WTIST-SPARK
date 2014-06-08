import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicMatMul{
  def main(args: Array[String]){
    if (args.length != 3){
      System.err.println("Usage: BasicMatMul <input mat1> <input mat2> <output>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("BasicMatMul")
    val sc = new SparkContext(conf)
    val text1=sc.textFile(args(0))
    val text2=sc.textFile(args(1))
    val A=text1.map{x => val line=x.split(",");(line(1),(line(0),line(2).toDouble))}
    val B=text2.map{x => val line=x.split(",");(line(0),(line(1),line(2).toDouble))}
    val C=A.join(B).map({case (key,((rowInd,rowVal),(colInd,colVal))) =>
      ((rowInd,colInd),rowVal*colVal)}).reduceByKey(_ + _).map(x =>
      x._1._1+","+x._1._2+","+x._2.toString).saveAsTextFile(args(2))
  }
}