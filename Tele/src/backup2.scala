import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.IndexedSeqView
import breeze.linalg._

object Similarity{
  def multiMat( spark: SparkContext,
                mat1: RDD[(String, (String, String))],
                mat2: RDD[(String, (String, String))],
                method: String
                ): RDD[((String, String), Double)] = {
    var res: RDD[((String, String), Double)] = null
    val cntRow = mat1.map(item => item._2._1).distinct().count()
    val cntCol = mat2.map(item => item._2._1).distinct().count()
    val blockSize = 2000
    if ((cntRow / blockSize) * (cntCol / blockSize) > 1) {
      res = multiMatPartition(spark, mat1, mat2, cntRow, cntCol, blockSize, method)
    } else {
      res = multiMat0(spark, mat1, mat2, method)
    }
    res
  }

  def id2Part(id: String, partNum: Int): Long = {
    (id.hashCode() & 0x7FFFFFFF) % partNum
  }

  def splitMatrix(spark: SparkContext,
                  mat: RDD[(String, (String, String))],
                  keyCount: Long,
                  blockSize: Int
                   ): Array[RDD[(String, (String, String))]] = {
    val keyPart = ((keyCount + blockSize - 1)/ blockSize).toInt
    val bKeyPart = spark.broadcast(keyPart)
    val pMat = mat.map(item => (item._2._1, (item._1, item._2._2)))
    val keyRdds = ArrayBuffer[RDD[(String, (String, String))]]()
    for (i <- 0 until keyPart) {
      val bI = spark.broadcast(i)
      //hash的方式做矩阵拆分，经量使矩阵拆分得均匀，适用于稀疏矩阵
      var tMat = pMat.filter(item => id2Part(item._1, bKeyPart.value) == bI.value)
      tMat = tMat.map(item => (item._2._1, (item._1, item._2._2)))
      //tMat.checkpoint()
      keyRdds += tMat
    }
    keyRdds.toArray
  }

  def corrcoef( a: IndexedSeqView[Double, Array[Double]], b: IndexedSeqView[Double, Array[Double]] ) = {
    val N = a.size
    val s = 1.0 / N
    var i = 0
    var (ea, eb) = (0.0, 0.0)
    while ( i < N ) {
      ea += s * a(i)
      eb += s * b(i)
      i += 1
    }
    i = 0
    var rn = 0.0
    var (siga, sigb) = (0.0, 0.0)
    while ( i < N ) {
      val ai = (a(i) - ea)
      val bi = (b(i) - eb)
      rn += ai * bi
      siga += ( ai*ai )
      sigb += ( bi*bi )
      i += 1
    }
    rn / (math.sqrt(siga)*math.sqrt(sigb))
  }

  def simEst(a:String, b:String, method:String):Double={
    val array1 = a.split(",").map(_.toDouble)
    val array2 = b.split(",").map(_.toDouble)
    if (method == "ecludSim"){
      val vec1 = DenseVector(array1)
      val vec2 = DenseVector(array2)
      return 1.0/(1.0 + norm(vec1 - vec2))
    }
    else if(method == "pearsSim"){
      if(array1.length<3) return 1.0
      return 0.5+0.5*corrcoef(array1.view, array2.view)
    }
    else if(method == "cosSim"){
      val vec1 = DenseVector(array1)
      val vec2 = DenseVector(array2)
      return  0.5+0.5*(vec1 dot vec2)/(norm(vec1)*norm(vec2))
    }
    else return 0.0
  }

  def calSim(k1:String, k2:String, a:Array[(String,String)], b:Array[(String,String)], method:String):ArrayBuffer[((String, String), Double)]={
    val result = new ArrayBuffer[((String, String), Double)]
    println("calsim:")
    for(i <- 0 to a.length-1){
      for(j <- 0 to b.length-1){
        result += (((k1+"-"+a(i)._1,k2+"-"+b(j)._1),simEst(a(i)._2,b(j)._2,method)))
      }
    }
    result
  }

  def multiMat0( spark: SparkContext,
                 mat1: RDD[(String, (String, String))],
                 mat2: RDD[(String, (String, String))],
                 method: String
                 ): RDD[((String, String), Double)] = {
    val A = mat1.groupByKey()
    val B = mat2.groupByKey()
    val C=A.cartesian(B)
    val resMats = C.flatMap(x => calSim(x._1._1, x._2._1, x._1._2.toArray, x._2._2.toArray, method))
    resMats
  }

  def multiMatPartition(spark: SparkContext,
                        mat1: RDD[(String, (String, String))],
                        mat2: RDD[(String, (String, String))],
                        rowCount: Long,
                        colCount: Long,
                        blockSize: Int = 1000,
                        method: String
                         ): RDD[((String, String), Double)] = {
    var subMats = ArrayBuffer[RDD[((String, String), Double)]]()
    val rowRdds = splitMatrix(spark, mat1, rowCount, blockSize)
    val colRdds = splitMatrix(spark, mat2, colCount, blockSize)
    for (i <- 0 until rowRdds.length) {
      val tMat1 = rowRdds(i)
      for (j <- 0 until colRdds.length) {
        val tMat2 = colRdds(j)
        val tRes = multiMat0(spark, tMat1, tMat2, method)
        //tRes.checkpoint()
        subMats += tRes
      }
    }
    val mulRes = subMats.reduce((mat1, mat2) => mat1.union(mat2))
    mulRes
  }

  def main(args: Array[String]){
    if (args.length != 4){
      System.err.println("Usage: Similarity <InputFile> <Separator> <EstMethod[ecludSim,pearsSim,cosSim]> <Output>")
      System.exit(1)
    }
    if (!Array("ecludSim","pearsSim","cosSim").exists(s => s == args(2))){
      System.err.println("EstMethod: ecludSim; pearsSim; cosSim")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Similarity")
    val sc = new SparkContext(conf)
    val text=sc.textFile(args(0))
    val mat1=text.map{x => val items=x.split(args(1)); val keys=items(0).split(",");(keys(0),(keys(1),items(1)))}
    val mat2=text.map{x => val items=x.split(args(1)); val keys=items(0).split(",");(keys(0),(keys(1),items(1)))}
    multiMat(sc, mat1, mat2, args(2)).map(x => x._1._1+","+x._1._2+"\t"+x._2).saveAsTextFile(args(3))
  }
}