import scala.util.parsing.json.{JSON, JSONObject}
import scala.beans.BeanProperty

object ScalaTest {
  def main(args: Array[String]): Unit = {
    val str_1 = "934|61|M|engineer|22902"

    println(str_1.split("|",0).apply(2))

    var array2 = Array(1, 2, 3, 4, 5)
    println(array2.apply(4))
    def regJson(json: Option[Any]) = json match {
      case Some(map: Map[String, Any]) => map
    }

    val str = "{\"name\":\"Justin\", \"age\":19,\"time\":\"2019-06-22 01:45:52.478\",\"time1\":\"2019-06-22 02:45:52.478\"}"
    val jsonS = JSON.parseFull(str)
    //val first = regJson(jsonS)
    print(jsonS)
    "hello".map(c => c.toUpper)
    "hello".map {
      _.toUpper
    }
    "hello".map { c => (c.toString) }

    def toLower(c: Char): Char = {
      c.toLower
    }

    def toLower2(c: Char) = (c: Char) => (c.toLower)

    "hello".map(toLower)
    "hello" (1)
    var a: String = "2"
    //    class Persion(var name:String =_,age:Int = _ ){
    //
    //    }
    class Persion1() {
      @BeanProperty
      var name: String = ""
      @BeanProperty
      var age: Int = 0

      override def toString() = "name" + name

    }
    print("=======")
    print(new Persion1().setName("alex").toString)
    var x = 1 to 20 toList
    var y = (1 to 20) toList

    for (s <- "str") {
      print(s)
    }
    ""

    val names = Map("key1" -> "value1", "key2" -> "value2")

    val names1: Map[String, String] = names


    names.map(x => print(x._1, x._2))
    for ((key, value) <- names) {
      print(key, value)
    }

    for (i <- 1 to 10 if i % 2 == 0) print(i)

    try {

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

    print(None != JSON.parseFull("{1\"name\":\"Justin\", \"age\":19,\"time\":\"2019-06-22 01:45:52.478\",\"time1\":\"2019-06-22 02:45:52.478\"}"))

    try {

    } catch {
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }

    }

  }
}
