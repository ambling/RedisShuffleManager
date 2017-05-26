package org.apache.spark.shuffle.redis

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriter}
import org.apache.spark.{SparkEnv, TaskContext}
import redis.clients.jedis.Jedis

class RedisShuffleWriter[K, V](
  handle: ShuffleHandle,
  mapId: Int,
  context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.asInstanceOf[RedisShuffleHandle[Any, Any, Any]].dependency

  private val blockManager = SparkEnv.get.blockManager

  private var output: Jedis = null

  private var sorter: RedisSorter[Any, Any, Any] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {

    sorter.insertAll(records)

    output = new Jedis
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new RedisSorter(
        output, mapId, context, dep.aggregator,
        Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new RedisSorter(
        output, mapId, context, None, Some(dep.partitioner), None, dep.serializer)
    }

    val partitionLengths = sorter.insertAll(records)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)

  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      if (output != null) {
        output.close()
      }
      if (sorter != null) {
        sorter.close()
      }
    }
  }
}
