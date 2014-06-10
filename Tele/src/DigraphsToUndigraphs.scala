import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer

object DigraphToUndigraph{

  def calculate(item: Array[String], args: Array[String]): ArrayBuffer[Double]={
    val weightNum = args.length-3
    val itemNum = item.length
    val textWeightNum = item(0).split(',').length
    if (weightNum != textWeightNum){
      System.err.println("The attribute number doesn't match the real number:"+weightNum+','+textWeightNum)
      System.exit(1)
    }
    val totalArray1 = new ArrayBuffer[Array[Double]]  // array nest to store the raw data
    for (i <- 0 until itemNum){
      totalArray1 += item(i).split(',').map(x => x.toDouble)
    }

    val totalArray2 = new ArrayBuffer[ArrayBuffer[Double]]   //  every subArray is an attribute vector
    for (i <- 0 until weightNum){
      val subArray = new ArrayBuffer[Double]
      for (j <- 0 until itemNum){
        subArray += totalArray1(j)(i)
      }
      totalArray2 += subArray
    }

    val outArray = new ArrayBuffer[Double]
    for (i <- 0 until weightNum){
      if (args(i+3) == "sum"){
        outArray += totalArray2(i).sum
      }
      else if (args(i+3) == "mean"){
        outArray += totalArray2(i).sum/itemNum
      }
      else if (args(i+3) == "max"){
        outArray += totalArray2(i).max
      }
      else if (args(i+3) == "min"){
        outArray += totalArray2(i).min
      }
    }
    return outArray
  }


  def main(args: Array[String]){
    if (args.length < 3){
      System.err.println("Usage: DigraphToUndigraph <InputFile> <Separator> <OutFile> [Weight]")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("DigraphToUndigraph")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))

    if (args.length == 3) {
      lines.flatMap { x =>
          val items = x.split(args(1))(0).split(',')
          Seq((items(0) + ',' + items(1), 1), (items(1) + ',' + items(0), 1))
      }.reduceByKey(_ + _).map(x => x._1+'\t'+x._2).saveAsTextFile(args(2))
    }
    else{
      lines.flatMap{ x =>
          val items = x.split(args(1))
          val keys = items(0).split(',')
          Seq((keys(0)+','+keys(1),items(1)),(keys(1)+','+keys(0),items(1)))
      }.groupByKey().map{ x =>
        (x._1,calculate(x._2.toArray,args))
      }.map{x =>
        x._1+'\t'+x._2.mkString(",")}.saveAsTextFile(args(2))
    }
    sc.stop()
  }

}