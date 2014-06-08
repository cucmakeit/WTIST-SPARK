object Jaccard {

  def Jaccard(a:String, b:String):Double={
    val array1 = a.split(",")
    val array2 = b.split(",")
    val len1 = array1.length
    val len2 = array2.length
    var intersection = 0;
    var union = 0;
    var i = 0;
    var j = 0;

    while (i<len1 && j < len2){
      if(array1(i).compareTo(array2(j))< 0) {
         i += 1
      }
      else if(array1(i).compareTo(array2(j))> 0){
         j += 1
      }
      else {
         i += 1
         j += 1
         intersection += 1
      }
    }

    union = len1 + len2 - intersection;
    println(union)
    println(intersection)
    var result = 0.0

    if(union != 0){
      result = intersection / union.toDouble
      return result
    }

    return result
  }
  
  def main(args: Array[String]): Unit = {
    val a = "1,3,5,7,9"
    val b = "1,4,5,6"
    val j = Jaccard(a,b)
    println(j)
  }
  
}