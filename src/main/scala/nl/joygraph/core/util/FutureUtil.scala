package nl.joygraph.core.util

import java.util.concurrent.CountDownLatch

import scala.concurrent.{ExecutionContext, Future}

object FutureUtil {
  def callbackOnAllComplete[T <: Any, R <: Any](futures : Iterable[Future[T]])(anyFunc : => R)(implicit executor: ExecutionContext): Future[R] = Future {
    val numFutures = futures.size
    val latch = new CountDownLatch(numFutures)
    futures.foreach(_ => latch.countDown())
    // todo on fail
    latch.await()
    anyFunc
  }
}
