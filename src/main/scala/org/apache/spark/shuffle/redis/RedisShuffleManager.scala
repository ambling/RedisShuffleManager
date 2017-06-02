package org.apache.spark.shuffle.redis

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import _root_.redis.clients.jedis.Jedis
import org.apache.spark.serializer.SerializerInstance

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

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
        RedisShuffleManager.mapKeyIter(shuffleId, mapId, numPartition).foreach { key =>
          jedis.del(key)
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
    s"SHUFFLE_${shuffleId}_${mapId}_"

  def mapKey(shuffleId: Int, mapId: Int, partition: Int) =
    s"SHUFFLE_${shuffleId}_${mapId}_${partition}".getBytes()

  def mapKeyIter(shuffleId: Int, mapId: Int, numPartitions: Int): Iterator[Array[Byte]] = {
    (0 until numPartitions).toIterator.map { partition =>
      mapKey(shuffleId, mapId, partition)
    }
  }

  def serializePair[K: ClassTag, V: ClassTag](serializer: SerializerInstance,
                                              key: K, value: V): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val sstream = serializer.serializeStream(baos)
    sstream.writeKey(key)
    sstream.writeValue(value)
    sstream.flush()
    sstream.close()
    baos.toByteArray
  }

  def deserializePair[K, V](serializer: SerializerInstance, data: Array[Byte]): (K, V) = {
    val bais = new ByteArrayInputStream(data)
    val sstream = serializer.deserializeStream(bais)
    val key: K = sstream.readKey()
    val value: V = sstream.readValue()
    sstream.close()
    (key, value)
  }
}