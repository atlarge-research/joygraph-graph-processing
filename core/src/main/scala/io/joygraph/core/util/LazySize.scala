package io.joygraph.core.util

/**
  * For iterables it's convenient to call the .size method, but for
  * comparison it's not always necessary to count all entries
  */
object LazySize{

  def sizeGreaterThan(target : Iterable[_], other : Int): Boolean = {
    var counter = 0
    for (x <- target) {
      counter += 1
      if (counter > other) {
        return true
      }
    }
    false
  }

  def sizeSmallerThan(target : Iterable[_],other : Int) : Boolean = {
    var counter = 0
    for (x <- target) {
      counter += 1
      if (counter >= other) {
        return false
      }
    }
    true
  }

  def sizeEquals(target : Iterable[_], other : Int) : Boolean = {
    var counter = 0
    for (x <- target) {
      counter += 1
      if (other == counter) {
        return true
      }
    }
    false
  }

}
