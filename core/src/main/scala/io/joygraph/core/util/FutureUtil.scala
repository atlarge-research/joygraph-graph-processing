package io.joygraph.core.util

import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

object FutureUtil {
  def callbackOnAllComplete[T <: Any, R <: Any](futures : Iterable[Future[T]])(anyFunc : => R)(implicit executor: ExecutionContext): Future[R] = Future {
    val numFutures = futures.size
    val latch = new CountDownLatch(numFutures)
    futures.foreach(_.recover{
      case t : Throwable =>
        t.printStackTrace()
    })
    futures.foreach(_.foreach(_ => latch.countDown()))
    // todo on fail
    latch.await()
    anyFunc
  }

  def callbackOnAllCompleteWithResults[T <: Any, R <: Any](futures : Iterable[Future[T]])(anyFunc : Iterable[T] => R)(implicit executor: ExecutionContext): Future[R] = Future {
    val numFutures = futures.size
    val latch = new CountDownLatch(numFutures)
    futures.foreach(_.recover{
      case t : Throwable =>
        t.printStackTrace()
    })
    futures.foreach(_.foreach(_ => latch.countDown()))
    // todo on fail
    println("Waiting is a bitch")
    latch.await()
    println("done waiting")
    val results = futures.map(Await.result(_, 0 nanos))
    println("Got results?")
    anyFunc(results)
  }
}
