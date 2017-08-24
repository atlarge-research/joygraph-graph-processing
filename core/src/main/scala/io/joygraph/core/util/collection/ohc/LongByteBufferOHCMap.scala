package io.joygraph.core.util.collection.ohc

import io.joygraph.core.util.collection.OHCWrapper

class LongByteBufferOHCMap extends OHCWrapper[Long](new LongCacheSerializer)
