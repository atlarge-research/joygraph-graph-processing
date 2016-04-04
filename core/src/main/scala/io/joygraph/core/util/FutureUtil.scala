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
    val res = try {
      anyFunc
    } catch {
      case t : Throwable =>
        t.printStackTrace()
        throw t
    }
    res
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
    println("Waiting for futures to complete")
    latch.await()
    println("Futures are complete")
    val results = futures.map(Await.result(_, 0 nanos))
    println("Results retrieved")
    val res = try {
      anyFunc(results)
    } catch {
      case t : Throwable =>
        t.printStackTrace()
        throw t
    }
    res
  }
}
