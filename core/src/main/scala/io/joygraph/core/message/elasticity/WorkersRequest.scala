package io.joygraph.core.message.elasticity

import com.typesafe.config.Config

case class WorkersRequest(jobConf : Config, numWorkers : Int) {

}
