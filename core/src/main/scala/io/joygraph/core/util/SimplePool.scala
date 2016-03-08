package io.joygraph.core.util

import java.util.concurrent.LinkedBlockingQueue

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


}
