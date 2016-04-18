package io.joygraph.core.actor.messaging

case class Message[I](var dst : I, var msg : Any)
