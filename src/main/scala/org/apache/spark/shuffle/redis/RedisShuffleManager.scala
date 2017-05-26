package org.apache.spark.shuffle.redis

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import _root_.redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

/**
  * A shuffler manager that support using local Redis store instead of Spark's BlockStore.
  */
class RedisShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {


  /**
    * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, ConcurrentHashMap[Int, Int]]()

  override def registerShuffle[K, V, C](
      shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new RedisShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {

    val numPartition =
      handle.asInstanceOf[RedisShuffleHandle[K, V, _]].dependency.partitioner.numPartitions
    val newMap = new ConcurrentHashMap[Int, Int]()
    val map = numMapsForShuffle.putIfAbsent(handle.shuffleId, newMap)
    if (map == null) newMap.put(mapId, numPartition)
    else map.put(mapId, numPartition)
    new RedisShuffleWriter(handle, mapId, context)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int,
                               context: TaskContext): ShuffleReader[K, C] = {
    new RedisShuffleReader[K, C](
      handle.asInstanceOf[RedisShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // delete map output in Redis
    val jedis = new Jedis
    Option(numMapsForShuffle.remove(shuffleId)).foreach { maps =>
      maps.asScala.foreach { map =>
        val mapId = map._1
        val numPartition = map._2
        (0 until numPartition).foreach { partition =>
          val mapKey = RedisShuffleManager.mapKey(shuffleId, mapId, partition)
          val keys = jedis.smembers(mapKey).asScala.toSeq
          keys.foreach { key =>
            val fullKey = RedisShuffleManager.fullKey(shuffleId, mapId, partition, key)
            jedis.del(fullKey)
          }
          jedis.del(mapKey)
        }
      }
    }
    jedis.close()
    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    throw new UnsupportedOperationException(
      "The RedisShuffleManager dose not return BlockResolver.")
  }

  override def stop(): Unit = {
    // do nothing
  }
}

object RedisShuffleManager {

  def mapKeyPrefix(shuffleId: Int, mapId: Int) =
    s"SHUFFLE_KEYS_${shuffleId}_${mapId}_"

  def mapKey(shuffleId: Int, mapId: Int, partition: Int) =
    s"SHUFFLE_KEYS_${shuffleId}_${mapId}_${partition}".getBytes()

  def mapKeyIter(shuffleId: Int, mapId: Int, numPartitions: Int): Iterator[Array[Byte]] = {
    (0 until numPartitions).toIterator.map { partition =>
      mapKey(shuffleId, mapId, partition)
    }
  }

  def keyPrefix(shuffleId: Int, mapId: Int, partition: Int): Array[Byte] = {
    //"SHUFFLE:${shuffleId}:${mapId}:${partition}:"
    val prefix = ByteBuffer.allocate(8+4+1+4+1+4+1)
    prefix.put("SHUFFLE:".getBytes)
      .putInt(shuffleId).put(':'.toByte)
      .putInt(mapId).put(':'.toByte)
      .putInt(partition).put(':'.toByte)
    prefix.array()
  }

  def fullKey(shuffleId: Int, mapId: Int, partition: Int, key: Array[Byte]): Array[Byte] = {
    val prefix = keyPrefix(shuffleId, mapId, partition)
    mergeKey(prefix, key)
  }

  def mergeKey(prefix: Array[Byte], key: Array[Byte]): Array[Byte] = {
    val keyStr = new Array[Byte](prefix.length + key.length)
    System.arraycopy(prefix, 0, keyStr, 0, prefix.length)
    System.arraycopy(key, 0, keyStr, prefix.length, key.length)
    keyStr
  }
}