package org.apache.spark.shuffle.redis

import java.nio.ByteBuffer
import java.util.Comparator

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.{PartitionedAppendOnlyMap, PartitionedPairBuffer, Spillable, WritablePartitionedPairCollection}
import org.apache.spark.{Aggregator, Partitioner, SparkEnv, TaskContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
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

  private var _peakMemoryUsedBytes: Long = 0L

  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

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

    val partitionLengths = new Array[Long](numPartitions)

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

      // combine the spilled data
      (0 until numPartitions).foreach { partition =>
        val mapKey = RedisShuffleManager.mapKey(shuffleId, mapId, partition)
        val keys = jedis.smembers(mapKey).asScala
        keys.foreach { key: Array[Byte] =>
          val fullKey = RedisShuffleManager.fullKey(shuffleId, mapId, partition, key)
          var combinedStr = jedis.lpop(fullKey)
          var combined = serialize.deserialize[C](ByteBuffer.wrap(combinedStr))
          var valueStr = jedis.lpop(fullKey)
          while (valueStr != null) {
            val value = serialize.deserialize[C](ByteBuffer.wrap(valueStr))
            combined = aggregator.get.mergeCombiners(combined, value)
            valueStr = jedis.lpop(fullKey)
          }

          combinedStr = serialize.serialize(combined).array()
          jedis.lpush(fullKey, combinedStr)
          partitionLengths(partition) += combinedStr.length
        }
      }

    } else {
      // directly store to the list in Redis
      while (records.hasNext) {
        val kv = records.next().asInstanceOf[Product2[K, V]]
        val partitionId = getPartition(kv._1)
        val length = put(partitionId, kv._1, kv._2)
        partitionLengths(partitionId) += length
      }
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

  private def put[T: ClassTag](partition: Int, k: K, v: T): Long = {
    require(partition >= 0 && partition < numPartitions,
      s"partition Id: ${partition} should be in the range [0, ${numPartitions})")

    val mapKey = RedisShuffleManager.mapKey(shuffleId, mapId, partition)

    val key = serialize.serialize(k: K).array()
    val fullKey = RedisShuffleManager.fullKey(shuffleId, mapId, partition, key)

    val value = serialize.serialize(v).array()
    jedis.lpush(fullKey, value) // just push to a list
    jedis.sadd(mapKey, key) // add this key (without prefix) to a set for query
    value.length
  }

  override protected def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = collection.partitionedDestructiveSortedIterator(comparator)

    while (inMemoryIterator.hasNext) {
      val nextItem = inMemoryIterator.next()
      val ((partitionId, key), value) = nextItem
      put(partitionId, key, value)
    }
  }

  override protected def forceSpill(): Boolean = {
    false
  }

  /**
    * clean Redis if error occurs
    */
  def clean(): Unit = {
    RedisShuffleManager.mapKeyIter(shuffleId, mapId, numPartitions).foreach { mapKey =>
      val keys = jedis.smembers(mapKey).asScala.toSeq
      if (keys.nonEmpty) jedis.del(keys:_*)
      jedis.del(mapKey)
    }
  }
}
