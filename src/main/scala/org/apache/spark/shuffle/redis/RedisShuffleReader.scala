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
    val blocks = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)

    blocks.toIterator.flatMap { block =>
      val host = block._1.host
      val jedis = new Jedis(host)
      val kv = block._2.toIterator.flatMap { mapTask =>
        val mapId = mapTask._1.asInstanceOf[ShuffleBlockId].mapId
        val mapKey = s"SHUFFLE_${mapId}_KEYS".getBytes
        val keys = jedis.smembers(mapKey)
        keys.iterator.asScala.flatMap {key =>
          val keyPos = key.lastIndexOf(":")
          val keyStr = key.slice(keyPos+1, key.length)
          val k: K = serialize.deserialize(ByteBuffer.wrap(keyStr))
          val count = jedis.llen(key)
          (0L until count.longValue()).toIterator.map {_ =>
            val valueStr = jedis.get(key)
            val value: C = serialize.deserialize(ByteBuffer.wrap(valueStr))
            (k, value)
          }
        }
      }
      jedis.close()
      kv
    }
  }
}
