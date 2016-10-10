package io.joygraph.core.partitioning.impl

import com.typesafe.config.Config
import io.joygraph.core.partitioning.VertexPartitioner

class NaiveHashPartitioner(prev : VertexPartitioner, oldWorkers : Int, newWorkers : Int) extends VertexPartitioner {
  override def init(conf: Config): Unit = {

  }

  override def destination(vId: Any): Int = {
    val vIdHash = vId.hashCode()
    if(vIdHash % newWorkers >= oldWorkers) {
      vIdHash % newWorkers
    } else if(oldWorkers > newWorkers) {
      vIdHash % newWorkers
    } else {
      prev.destination(vId)
    }
  }
}
