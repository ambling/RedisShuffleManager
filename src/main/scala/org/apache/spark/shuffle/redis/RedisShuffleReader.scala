package org.apache.spark.shuffle.redis

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.storage.{BlockManager, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import redis.clients.jedis.Jedis

/**
  * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
  * requesting them from other nodes' Redis store.
  */
private[spark] class RedisShuffleReader[K, C](
  handle: RedisShuffleHandle[K, _, C],
  startPartition: Int,
  endPartition: Int,
  context: TaskContext,
  serializerManager: SerializerManager = SparkEnv.get.serializerManager,
  blockManager: BlockManager = SparkEnv.get.blockManager,
  mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  val serialize = handle.dependency.serializer.newInstance()
  private val dep = handle.dependency

  override def read(): Iterator[Product2[K, C]] = {
    val shuffleId = handle.shuffleId
    val blocks = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
    val grouped = blocks.groupBy(_._1.host).map {b => {
      val all = b._2.flatMap { block =>
        block._2.flatMap { mapTask =>
          val size = mapTask._2
          if (size > 0) {
            val partition = mapTask._1.asInstanceOf[ShuffleBlockId].reduceId
            val mapId = mapTask._1.asInstanceOf[ShuffleBlockId].mapId
            Some((partition, mapId, size))
          } else {
            None
          }
        }
      }
      (b._1, all)
    }}

    val iters = grouped.map { block =>
      new Iterator[(K, C)] {
        val host = block._1
        var jedis: Jedis = null
        var kv: Iterator[(K, C)] = Iterator.empty
        var first = true

        override def hasNext: Boolean = {
          if (first) {
            //initialize, may be we need to check the thread safety
            first = false
            jedis = new Jedis(host)
            kv = block._2.toIterator.flatMap { mapTask =>
              val partition = mapTask._1
              val mapId = mapTask._2
              val mapKey = RedisShuffleManager.mapKey(shuffleId, mapId, partition)
              val length = jedis.llen(mapKey)
              (0L until length).toIterator.flatMap { idx =>
                val data = jedis.rpoplpush(mapKey, mapKey)
                if (data == null) None
                else {
                  val (k, v) = RedisShuffleManager.deserializePair[K, C](serialize, data)
                  Some((k, v))
                }
              }
            }

            kv.hasNext
          } else {
            jedis != null && kv.hasNext
          }
        }

        override def next(): (K, C) = {
          try {
            kv.next()
          } finally {
            if (!hasNext && jedis != null) {
              jedis.close()
              jedis = null
            }
          }
        }
      }
    }
    val recordIter = iters.foldLeft(Iterator[(K, C)]())(_ ++ _)

    // The rest code are copied from Spark's BlockStoreShuffleReader, maybe we can reuse it

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }

}
