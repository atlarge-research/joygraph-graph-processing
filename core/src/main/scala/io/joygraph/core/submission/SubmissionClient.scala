package io.joygraph.core.submission

trait SubmissionClient {
  def submit() : Unit
  def submitBlocking() : Unit
}
