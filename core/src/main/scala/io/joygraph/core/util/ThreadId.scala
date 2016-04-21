package io.joygraph.core.util

import java.util.concurrent.atomic.AtomicInteger

object ThreadId {
  private val nextId : AtomicInteger = new AtomicInteger(0)

  // Thread local variable containing each thread's ID
  private val threadId : ThreadLocal[Int] = new ThreadLocal[Int]() {
    // TODO id can go > Integer max
    override def initialValue(): Int = nextId.getAndIncrement()
  }

  def getId = threadId.get()

  def getMod(numThreads : Int) = threadId.get() % numThreads

  def apply[T](numThreads : Int)(f: Int => T) = {
    val threadId = getMod(numThreads : Int)
    f(threadId)
  }
}