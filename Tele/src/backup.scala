import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.ClassTag
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

object MatMultiply{
  def multiMat[T: ClassTag](
                             spark: SparkContext,
                             mat1: RDD[(T, (T, Double))],
                             mat2: RDD[(T, (T, Double))]
                             ): RDD[((T, T), Double)] = {
    var res: RDD[((T, T), Double)] = null
    val cntRow = mat1.map(item => item._2._1).distinct().count()
    val cntCol = mat2.map(item => item._2._1).distinct().count()
    //拆分矩阵中，每个小矩阵的维度设定为100000
    val blockSize = 2000
    //如果维度少于100000,就没有必要再做矩阵拆分计算了
    if ((cntRow / blockSize) * (cntCol / blockSize) > 1) {
      res = multiMatPartition(spark, mat1, mat2, cntRow, cntCol, blockSize)
    } else {
      res = multiMat0(spark, mat1, mat2)
    }
    res
  }

  def id2Part[T](id: T, partNum: Int)(implicit cm: ClassTag[T]): Long = {
    (id.hashCode() & 0x7FFFFFFF) % partNum
  }

  def splitMatrix[T: ClassTag](spark: SparkContext,
                               mat: RDD[(T, (T, Double))],
                               keyCount: Long,
                               blockSize: Int
                                ): Array[RDD[(T, (T, Double))]] = {
    val keyPart = ((keyCount + blockSize - 1)/ blockSize).toInt
    val bKeyPart = spark.broadcast(keyPart)
    val pMat = mat.map(item => (item._2._1, (item._1, item._2._2)))
    val keyRdds = ArrayBuffer[RDD[(T, (T, Double))]]()
    for (i <- 0 until keyPart) {
      val bI = spark.broadcast(i)
      //hash的方式做矩阵拆分，经量使矩阵拆分得均匀，适用于稀疏矩阵
      var tMat = pMat.filter(item => id2Part(item._1, bKeyPart.value) == bI.value)
      tMat = tMat.map(item => (item._2._1, (item._1, item._2._2)))
      //tMat.checkpoint()
      tMat.foreach{(_)=>{}}
      keyRdds += tMat
    }
    keyRdds.toArray
  }

  def multiMat0[T:ClassTag]( spark: SparkContext,
                             mat1: RDD[(T, (T, Double))],
                             mat2: RDD[(T, (T, Double))]
                             ): RDD[((T, T), Double)] = {
    val resMats = mat1.join(mat2).map({case (key,((rowInd,rowVal),(colInd,colVal))) =>
      ((rowInd,colInd),rowVal*colVal)}).reduceByKey(_ + _)
    resMats
  }

  def multiMatPartition[T: ClassTag](spark: SparkContext,
                                     mat1: RDD[(T, (T, Double))],
                                     mat2: RDD[(T, (T, Double))],
                                     rowCount: Long,
                                     colCount: Long,
                                     blockSize: Int = 1000
                                      ): RDD[((T, T), Double)] = {
    var subMats = ArrayBuffer[RDD[((T, T), Double)]]()
    val rowRdds = splitMatrix(spark, mat1, rowCount, blockSize)
    val colRdds = splitMatrix(spark, mat2, colCount, blockSize)
    for (i <- 0 until rowRdds.length) {
      val tMat1 = rowRdds(i)
      for (j <- 0 until colRdds.length) {
        val tMat2 = colRdds(j)
        val tRes = multiMat0(spark, tMat1, tMat2)
        //tRes.checkpoint()
        tRes.foreach{(_)=>{}}
        subMats += tRes
      }
    }
    val mulRes = subMats.reduce((mat1, mat2) => mat1.union(mat2))
    mulRes
  }

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("MatMultiply")
    val sc = new SparkContext(conf)
    val text1=sc.textFile(args(0))
    val text2=sc.textFile(args(1))
    //sc.setCheckpointDir(args(2)+"_/checkpoint")
    val mat1=text1.map{x => val line=x.split(",");(line(1).toLong,(line(0).toLong,line(2).toDouble))}
    val mat2=text2.map{x => val line=x.split(",");(line(0).toLong,(line(1).toLong,line(2).toDouble))}
    val mat3=multiMat(sc, mat1, mat2).map(x => x._1._1+","+x._1._2+","+x._2)
    mat3.saveAsTextFile(args(2))
  }
}