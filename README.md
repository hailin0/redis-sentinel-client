
# redis-sentinel-client
	redis-sentinel(哨兵)模式的分片连接池，提供与ShardedJedisPool一样的API，并根据sentinel集群的master变更自动切换主从。


# 简述
	redis2.x的高可用方案sentinel（哨兵），本身提供redis主从集群的自动管理功能，以及主从自动切换的高可用功能。
	而在java客户端使用时需要用到sentinel的客户端，因为调用方式不一样，直接使用jedis老的连接池是无法做到主从切换的效果。
	jedis客户端中自带了一个sentinel单机版连接池实现redis.clients.jedis.JedisSentinelPool.java，
	使用JedisSentinelPool.java可以很轻松的使用sentinel的功能，达到主从切换的效果，但是没有提供分片的连接池。
	
	redis.clients.jedis.ShardedJedisSentinelPool.java就是在redis.clients.jedis.JedisSentinelPool.java的基础上，
	增加了对分片的支持，可以很少的改动老代码进行升级。


# 测试代码
         // sentinel配置文件中的master-name列表
        List<String> masters = new ArrayList<String>();
        masters.add("master1");
        masters.add("master2");

        // sentinel集群列表
        Set<String> sentinels = new HashSet<String>();
        sentinels.add("192.168.1.112:26379");

        //初始化连接池
        ShardedJedisSentinelPool pool = new ShardedJedisSentinelPool(masters, sentinels);
        
        //获取jedis客户端
        ShardedJedis jedis = pool.getResource();

# 代码实现
<a href="https://github.com/hailin0/redis-sentinel-client/blob/master/src/main/java/redis/clients/jedis/JedisSentinelPool.java">单机连接池-JedisSentinelPool.java</a>
<br>
<a href="https://github.com/hailin0/redis-sentinel-client/blob/master/src/main/java/redis/clients/jedis/ShardedJedisSentinelPool.java">分片连接池-ShardedJedisSentinelPool.java</a>
