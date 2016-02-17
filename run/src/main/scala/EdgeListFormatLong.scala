import nl.joygraph.core.format.EdgeListFormat

class EdgeListFormatLong extends EdgeListFormat[Long] {
  override def stringToI(vId: String): Long = vId.toLong
}
