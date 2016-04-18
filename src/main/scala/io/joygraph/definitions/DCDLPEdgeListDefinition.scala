package io.joygraph.definitions

import java.nio.charset.StandardCharsets

import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.ParseUtil
import io.joygraph.programs.DCDLP

class DCDLPEdgeListDefinition extends ProgramDefinition[String, Long, Long, Boolean] (
  (l,v) => ParseUtil.edgeListLineLongLongBooleanWithDefault(l,v,DCDLP.UNIDIRECTIONAL),
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[DCDLP]
)
