package io.joygraph.definitions

import java.nio.charset.StandardCharsets

import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.ParseUtil
import io.joygraph.programs.SSSP

class SSSPEdgeListDoubleDefinition extends ProgramDefinition[String, Long, Double, Double] (
  ParseUtil.edgeListLineLongLongDouble,
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[SSSP]
)