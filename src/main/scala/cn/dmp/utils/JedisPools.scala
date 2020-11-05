package cn.dmp.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object JedisPools {

    private val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "10.172.50.54", 6379)


    def getJedis() = jedisPool.getResource//拿到jedis连接

}
