class User(height: Int, age: Int) {
  val this.height = height
  val this.age = age;
  var name = ""

  def this() {
    this(5, 5)
  }

  def this(height: Int, age: Int, name: String) {
    this(5, 5)
    this.name = name
  }

}
