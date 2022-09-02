package geektime.streaming

import scala.annotation.{Annotation, StaticAnnotation}
import scala.beans.BeanProperty

class People  extends Annotation with StaticAnnotation{
  @BeanProperty
  var name:String ="";
  var  age:Int = 0

/*
  def setName(name: String): Unit = { this.name = name }
  def getName(): String = {this.name}
  def setAge(age:Int ) :Unit= {this.age =age}
  def getAge():Int ={ this.age}
*/

}
