package io.joygraph.core.actor.vertices

/**
  * Class used to reduce garbage
  */
case class VertexEdge[I, E](var src : I, var dst :I, var value : E)
