# RedisShuffleManager
A shuffle manager based on Redis for Apache Spark

It seems that, a simple repartition of 10,000,000 Ints with this shuffle manager is 10 times slower than Spark's sort-based shuffling...
