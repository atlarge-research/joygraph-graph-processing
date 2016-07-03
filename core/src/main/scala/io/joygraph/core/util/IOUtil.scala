package io.joygraph.core.util

import io.joygraph.core.message.NettyAddress

object IOUtil {
  private type Position = Long
  private type Length = Long

  // TODO find a better way
  // TODO abstract hostname transformation
  def infiniband(nettyAddress : NettyAddress) : NettyAddress = {
    val NettyAddress(host, port) = nettyAddress
    NettyAddress(if(host.startsWith("node")) host+".ib.cluster" else host, port)
  }

  def splits(length : Long, numSplits : Int, startPos : Long = 0): Array[(Position, Length)] = {
    val res = new Array[(Long, Long)](numSplits)

    var i = 0
    while (i < numSplits) {
      res(i) = split(i, length, numSplits, startPos)
      i += 1
    }

    res
  }

  def split(index : Int, length : Long, numSplits : Int, startPos : Long = 0) : (Position, Length) = {
    val position : Long =
      if (index == 0) {
        startPos
      } else {
        startPos + (length / numSplits) * index
      }
    val len: Long =
      if (index == numSplits - 1) {
        startPos + length - position
      } else {
        length / numSplits
      }
    (position, len)
  }
}
