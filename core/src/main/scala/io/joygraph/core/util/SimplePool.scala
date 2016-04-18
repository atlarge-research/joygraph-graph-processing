package io.joygraph.core.util

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConversions._

class SimplePool[T] (factory : => T){
  private[this] val pool : LinkedBlockingQueue[T] = new LinkedBlockingQueue[T]()

  def borrow() : T = {
    val pObject = pool.poll()
    if (pObject == null) {
      factory
    } else {
      pObject
    }
  }

  def release(pObject : T): Unit = {
    pool.put(pObject)
  }

  def foreach(f : T => Unit) = {
    pool.foreach(f)
  }

  def apply(f : T => Unit) = {
    val instance = borrow()
    f(instance)
    release(instance)
  }

}
