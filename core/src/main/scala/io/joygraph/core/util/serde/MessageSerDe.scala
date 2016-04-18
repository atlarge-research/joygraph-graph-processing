package io.joygraph.core.util.serde

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import io.joygraph.core.actor.messaging.Message
import io.joygraph.core.util.TypeUtil

class MessageSerDe[I](clazzI : Class[I]) {
  private[this] var voidOrUnitClass : Boolean = _
  private[this] var outgoingClass : Class[_] = _
  private[this] var incomingClass : Class[_] = _
  private[this] val message = Message[I](null.asInstanceOf[I], null)

  def setOutgoingClass(clazz : Class[_]) = {
    voidOrUnitClass = TypeUtil.unitOrVoid(clazz)
    outgoingClass = clazz
  }

  def setIncomingClass(clazz : Class[_]) = {
    voidOrUnitClass = TypeUtil.unitOrVoid(clazz)
    incomingClass = clazz
  }

  // TODO message deserialization step could be skipped if the size of the message would be known beforehand
  // but this introduces additional network cost
  def messagePairDeserializer(kryo : Kryo, input : Input) : Message[I] = {
    message.dst = kryo.readObject(input, clazzI)
    message.msg = messageDeserializer(kryo, input)
    message
  }

  def messageDeserializer(kryo : Kryo, input : Input) : Any = {
    kryo.readObject(input, outgoingClass)
  }

  def messagePairSerializer(kryo : Kryo, output : Output, o : Message[I]) = {
    kryo.writeObject(output, o.dst)
    messageSerializer(kryo, output, o.msg)
  }

  def messageSerializer(kryo : Kryo, output : Output, m : Any) = {
    kryo.writeObject(output, m)
  }

  def incomingMessageDeserializer(kryo : Kryo, input : Input) : Any = {
    kryo.readObject(input, incomingClass)
  }
}
