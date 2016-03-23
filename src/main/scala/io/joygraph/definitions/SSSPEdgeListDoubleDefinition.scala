package io.joygraph.definitions

import java.nio.charset.StandardCharsets

import io.joygraph.core.program.ProgramDefinition
import io.joygraph.programs.SSSP

class SSSPEdgeListDoubleDefinition extends ProgramDefinition[String, Long, Double, Double] (
  (l) => {
    val s = l.split("\\s")
    (s(0).toLong, s(1).toLong, s(2).toDouble)
  },
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[SSSP]
)