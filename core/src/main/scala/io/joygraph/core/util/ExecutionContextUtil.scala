package io.joygraph.core.util

import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}

object ExecutionContextUtil {
  def createForkJoinPoolWithPrefix(prefix : String) : ForkJoinPool = {
    new ForkJoinPool(Runtime.getRuntime.availableProcessors,
      new ForkJoinPool.ForkJoinWorkerThreadFactory {
        override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
          val t = new NamePrefixedForkJoinWorkerThread(pool)
          t.setName(prefix + t.getName)
          t
        }
      },
      null : Thread.UncaughtExceptionHandler,
      true)
  }

  class NamePrefixedForkJoinWorkerThread(pool : ForkJoinPool) extends ForkJoinWorkerThread(pool) {

  }
}
