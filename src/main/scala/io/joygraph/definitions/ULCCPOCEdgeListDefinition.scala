package io.joygraph.definitions

import java.nio.charset.StandardCharsets

import io.joygraph.core.program.ProgramDefinition
import io.joygraph.core.util.ParseUtil
import io.joygraph.programs._

class DLCCPOCEdgeListDefinition extends ProgramDefinition[String, Long, Double, Unit] (
  ParseUtil.edgeListLineLongLong,
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[DLCCPOC]
)

class ULCCPOCEdgeListDefinition extends ProgramDefinition[String, Long, Double, Unit] (
  ParseUtil.edgeListLineLongLong,
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[ULCCPOC]
)

class DLCCPOC2EdgeListDefinition extends ProgramDefinition[String, Long, Double, Unit] (
  ParseUtil.edgeListLineLongLong,
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[DLCCPOC2]
)

class ULCCPOC2EdgeListDefinition extends ProgramDefinition[String, Long, Double, Unit] (
  ParseUtil.edgeListLineLongLong,
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[ULCCPOC2]
)