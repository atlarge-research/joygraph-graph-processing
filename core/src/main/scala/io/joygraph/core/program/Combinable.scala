package io.joygraph.core.program

trait Combinable[M] {
  def combine(m1 : M, m2 : M) : M
}
