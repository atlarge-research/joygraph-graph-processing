package io.joygraph.programs

import io.joygraph.core.program.Combinable

class BFSCombinable extends BFS with Combinable[Long]{
  override def combine(m1: Long, m2: Long): Long = math.min(m1, m2)
}
