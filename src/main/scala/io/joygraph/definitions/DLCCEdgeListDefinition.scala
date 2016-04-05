package io.joygraph.definitions

import java.nio.charset.StandardCharsets

import io.joygraph.core.program.{NullClass, ProgramDefinition}
import io.joygraph.programs.DLCC

class DLCCEdgeListDefinition extends ProgramDefinition[String, Long, Double, NullClass] (
  (l) => {
    val s = l.split("\\s")
    (s(0).toLong, s(1).toLong, NullClass.SINGLETON)
  },
  (l) => l.toLong,
  (v, outputStream) =>
    outputStream.write(s"${v.id} ${v.value}\n".getBytes(StandardCharsets.UTF_8)),
  classOf[DLCC]
)
