package nl.joygraph.core.actor.state


object GlobalState extends Enumeration {
  val NONE, LOAD_DATA, SUPERSTEP, POST_SUPERSTEP = Value
}