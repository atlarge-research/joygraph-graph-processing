package io.joygraph.analysis

import com.typesafe.config.ConfigFactory
import io.joygraph.analysis.job.ElasticityPolicyAnalysis
import io.joygraph.core.actor.elasticity.policies.ElasticPolicy

import scala.io.Source

object JobAnalysis extends App {
  val resultsPath = "/home/sietse/thesis/experimental-results/elastic/GR26/results-GR26-PY-OWN-autoscale-BFS-PR-WCC-27-10-2016/b6541719573"

  def getAveragingPolicy(path : String) : ElasticPolicy = {
    val benchmarkLog = s"$path/benchmark-log.txt"
    val theLines = Source.fromFile(benchmarkLog).getLines()
    val policyClassLine = theLines.filter(_.contains("job.policy.class")).next()
    val thresholdLine = theLines.filter(_.contains("std-threshold")).next()
    val lowerboundLine = theLines.filter(_.contains("lowerbound")).next()

    val policyClass = policyClassLine.split("=")(1).trim
    val policy = Class.forName(policyClass).newInstance().asInstanceOf[ElasticPolicy]

    val config = ConfigFactory.parseString(
      thresholdLine
    )

    policy.init(config)
    policy
  }

  val analysis = ElasticityPolicyAnalysis(resultsPath, getAveragingPolicy(resultsPath))

  analysis.simulate(analysis.maxSteps)
}
