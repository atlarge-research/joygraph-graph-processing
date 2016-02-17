package nl.joygraph.core

import com.typesafe.config.Config
import nl.joygraph.core.actor.Worker
import nl.joygraph.core.program.VertexProgram

import scala.reflect._


object AppFactoryProvider {
  def workerFactory[I : ClassTag,V : ClassTag,E : ClassTag,M : ClassTag](config : Config, clazz : Class[_ <: VertexProgram[I,V,E,M]]): () => Worker[I,V,E,M] = () => {
    new Worker[I,V,E,M](config)
  }

//  def create[I,V,E,M](clazz : Class[_ <: VertexProgram[I,V,E,M]]) : AppFactoryProvider[I,V,E,M] = {
//    new AppFactoryProvider[I,V,E,M]
//  }
}

//protected[this] class AppFactoryProvider[I,V,E,M]  {
//
//  var clazzI : Class[I] = _
//  var clazzV : Class[V] = _
//  var clazzE : Class[E] = _
//  var clazzM : Class[M] = _

//  def initializeTypeTags[T : TypeTag](x : T = this) {
//    val targs: List[Type] = typeTag[T].tpe match { case TypeRef(_, _, args) => args }
//    targs.map(_.typeSymbol.asClass.fullName).map(Class.forName).zipWithIndex.foreach {
//      case (cls, i) =>
//        i match {
//          case 0 => clazzI = cls.asInstanceOf[Class[I]]
//          case 1 => clazzV = cls.asInstanceOf[Class[V]]
//          case 2 => clazzE = cls.asInstanceOf[Class[E]]
//          case 3 => clazzM = cls.asInstanceOf[Class[M]]
//        }
//    }
//  }

//}
