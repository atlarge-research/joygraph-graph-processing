package io.joygraph.core.util

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}

object ExecutionContextUtil {
  def createForkJoinPoolWithPrefix(prefix : String, numThreads : Int = Runtime.getRuntime.availableProcessors, uncaughtExceptionHandler: UncaughtExceptionHandler = null) : ForkJoinPool = {
    new ForkJoinPool(numThreads,
      new ForkJoinWorkerThreadFactoryWithPrefix(prefix),
      uncaughtExceptionHandler,
      true)
  }

  class NamePrefixedForkJoinWorkerThread(pool : ForkJoinPool) extends ForkJoinWorkerThread(pool) {

  }

  class ForkJoinWorkerThreadFactoryWithPrefix(prefix : String) extends ForkJoinPool.ForkJoinWorkerThreadFactory {
    override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
      val t = new NamePrefixedForkJoinWorkerThread(pool)
      t.setName(prefix + t.getName)
      t
    }
  }
}
