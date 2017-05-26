package org.apache.spark.shuffle.redis

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.storage.{BlockManager, ShuffleBlockId}
import org.apache.spark.{MapOutputTracker, SparkEnv, TaskContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._


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
              val keys = jedis.smembers(mapKey)
              keys.iterator.asScala.flatMap {key =>
                val fullKey = RedisShuffleManager.fullKey(shuffleId, mapId, partition, key)
                val k: K = serialize.deserialize(ByteBuffer.wrap(key))
                val count = jedis.llen(fullKey)
                (0L until count.longValue()).toIterator.flatMap {_ =>
                  val value = jedis.rpoplpush(fullKey, fullKey) // move to the end to avoid deletion
                  if (value == null) None
                  else {
                    val v: C = serialize.deserialize(ByteBuffer.wrap(value))
                    Some((k, v))
                  }
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
    iters.foldLeft(Iterator[(K, C)]())(_ ++ _)
  }

}
