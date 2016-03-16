package io.joygraph.core.util.net

import java.net.InetAddress

object NetUtils {
  def getHostName: String = {
    InetAddress.getLocalHost.getHostName
  }
}
