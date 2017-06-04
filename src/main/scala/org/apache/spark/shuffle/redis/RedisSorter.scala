package org.apache.spark.shuffle.redis

import java.util.Comparator

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.{PartitionedAppendOnlyMap, Spillable, WritablePartitionedPairCollection}
import org.apache.spark.{Aggregator, Partitioner, SparkEnv, TaskContext}
import redis.clients.jedis.{Jedis, Pipeline}

import scala.reflect.ClassTag

/**
  *
  * Sort and combine map output directly to Redis.
  */
class RedisSorter[K: ClassTag, V: ClassTag, C: ClassTag](
  jedis: Jedis,
  shuffleId: Int,
  mapId: Int,
  context: TaskContext,
  aggregator: Option[Aggregator[K, V, C]] = None,
  partitioner: Option[Partitioner] = None,
  ordering: Option[Ordering[K]] = None,
  serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
    with Logging {

  private val serialize = serializer.newInstance()

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }
  val partitionLengths = new Array[Long](numPartitions)

  private var _peakMemoryUsedBytes: Long = 0L

  @volatile private var map = new PartitionedAppendOnlyMap[K, C]

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  def insertAll(records: Iterator[Product2[Any, Any]]): Array[Long] = {
    val shouldCombine = aggregator.isDefined

    for (i <- 0 until partitionLengths.length) partitionLengths(i) = 0

    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        kv = records.next().asInstanceOf[Product2[K, V]]
        map.changeValue((getPartition(kv._1), kv._1), update)
        maybeSpillCollection()
      }
      spill(map) // force spill remaining data

      // TODO combine the spilled data to save network transfer


    } else {
      // directly store to the list in Redis
      var syncCnt = 0
      var pipe = jedis.pipelined()
      while (records.hasNext) {
        val kv = records.next().asInstanceOf[Product2[K, V]]
        val partitionId = getPartition(kv._1)
        val length = put(pipe, partitionId, kv._1, kv._2)
        partitionLengths(partitionId) += length // add to partition length
        syncCnt += 1
        if (syncCnt >= 10000) {
          syncCnt = 0
          pipe.sync()
          pipe = jedis.pipelined()
        }
      }
      pipe.sync()
    }
    partitionLengths
  }

  private def maybeSpillCollection(): Unit = {
    var estimatedSize = 0L
    estimatedSize = map.estimateSize()
    if (maybeSpill(map, estimatedSize)) {
      map = new PartitionedAppendOnlyMap[K, C]
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  private def put[T: ClassTag](pipe: Pipeline, partition: Int, k: K, v: T): Long = {
    require(partition >= 0 && partition < numPartitions,
      s"partition Id: ${partition} should be in the range [0, ${numPartitions})")

    val mapKey = RedisShuffleManager.mapKey(shuffleId, mapId, partition)
    val data = RedisShuffleManager.serializePair(serialize, k, v)
    pipe.rpush(mapKey, data)
    data.length
  }

  override protected def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = collection.partitionedDestructiveSortedIterator(comparator)

    val pipe = jedis.pipelined()
    while (inMemoryIterator.hasNext) {
      val nextItem = inMemoryIterator.next()
      val ((partitionId, key), value) = nextItem
      val length = put(pipe, partitionId, key, value)
      partitionLengths(partitionId) += length // add to partition length
    }
    pipe.sync()
  }

  override protected def forceSpill(): Boolean = {
    false
  }

  /**
    * clean Redis if error occurs
    */
  def clean(): Unit = {
    RedisShuffleManager.mapKeyIter(shuffleId, mapId, numPartitions).foreach { mapKey =>
      jedis.del(mapKey)
    }
  }
}
