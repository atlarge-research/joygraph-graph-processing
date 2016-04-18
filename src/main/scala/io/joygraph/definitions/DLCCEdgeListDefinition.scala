package io.joygraph.definitions

import java.nio.charset.StandardCharsets

import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.ParseUtil
import io.joygraph.programs.DLCC

class DLCCEdgeListDefinition extends ProgramDefinition[String, Long, Double, Unit] (
  ParseUtil.edgeListLineLongLong,
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[DLCC]
)
