import org.apache.spark.util.Utils

object HashCodeTest {
  def main(args: Array[String]): Unit = {

    try {


      var list = List("beijing-1", "beijing-2", "beijing-3", "shanghai-1", "shanghai-2", "tianjing-1", "tianjing-2");
      //    list.foreach(x => println((x,
      //      print(x.split("-")))))
      //x.split("-").hashCode/list.length)) )
    //  print("beijing-1".split("-").apply(0))

      val hashcode = nonNegativeMod("123".hashCode, 2)
      print(hashcode)
    } catch {
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }

    }
  }


  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }


}
