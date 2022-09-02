package geektime.optimize

import com.esotericsoftware.kryo.Kryo
import geektime.optimize.OptimizeKryo.{Persion1, Persion2}
import org.apache.spark.serializer.KryoRegistrator


//注册使用Kryo序列化的类，要求MyClass1和MyClass2必须实现java.io.Serializable
class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Persion1]);
    kryo.register(classOf[Persion2]);
  }
}
