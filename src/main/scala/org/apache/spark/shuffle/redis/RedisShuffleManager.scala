package org.apache.spark.shuffle.redis

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import _root_.redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * A shuffler manager that support using local Redis store instead of Spark's BlockStore.
  */
class RedisShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {


  /**
    * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override def registerShuffle[K, V, C](
      shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new RedisShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {

    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
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
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        val mapKey = s"SHUFFLE_${mapId}_KEYS".getBytes
        val keys = jedis.smembers(mapKey).asScala.toSeq
        jedis.del(keys:_*)
        jedis.del(mapKey)
      }
    }
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
