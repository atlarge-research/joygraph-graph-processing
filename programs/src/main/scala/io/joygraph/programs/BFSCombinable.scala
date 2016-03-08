package io.joygraph.programs

import io.joygraph.core.program.Combinable

class BFSCombinable extends BFS with Combinable[Int]{
  override def combine(m1: Int, m2: Int): Int = math.min(m1, m2)
}
