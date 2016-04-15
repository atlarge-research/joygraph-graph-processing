package io.joygraph.core.util.concurrency

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.LinkedBlockingQueue

import io.joygraph.core.util.concurrency.PartitionWorker.ForeverThread

object PartitionWorker {
  private[PartitionWorker] class ForeverThread(name : String, errorReporter : (Throwable) => Unit) extends Thread(name) {

    // TODO put in a sane number for capacity
    private[this] val taskQueue = new LinkedBlockingQueue[Runnable]()
    @volatile private[this] var running = true
    @volatile private[this] var taskIsRunning = false
    private[this] val waitObject = new Object

    sys.addShutdownHook{
        running = false
        interruptMe()
    }
    private[this] def interruptMe(): Unit = {
      interrupt()
    }

    override def run(): Unit = {
      try {
        var runnable : Runnable = null
        while (running) {
          runnable = taskQueue.take()
          taskIsRunning = true
          runnable.run()
          taskIsRunning = false
          if (taskQueue.isEmpty) {
            waitObject.synchronized{
              waitObject.notify()
            }
          }
        }
      } catch {
        case e : InterruptedException =>
        // noop
        case t : Throwable =>
          errorReporter(t)
      }
    }

    def execute(r : Runnable) : Unit = {
      // pass runnable to thread
      taskQueue.add(r)
    }
  }
}

class PartitionWorker(name : String, errorReporter : (Throwable) => Unit) {
  private[this] val EXCEPTIONHANDLER = new UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = errorReporter(e)
  }

  private[this] val foreverThread = new ForeverThread(name, errorReporter)
  foreverThread.start()

  def execute(runnable: Runnable): Unit = {
    foreverThread.execute(runnable)
  }

}
