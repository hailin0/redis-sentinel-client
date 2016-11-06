package com.hlin.redis_sentinel_client;

import java.util.Date;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisSentinelPool;

/**
 * 
 * 测试sentinel连接池读写与自动切换
 * 
 * @author hailin0@yeah.net
 * @createDate 2016年7月17日
 *
 */
public class SpringTest {

    static ShardedJedisSentinelPool pool = init();

    public static void main(String[] args) {

        test();
    }

    public static void test() {
        while (true) {

            for (int i = 0; i < 100; i++) {
                set(String.valueOf(i), String.valueOf(i) + new Date().toLocaleString());
            }
            for (int i = 0; i < 100; i++) {
                get(String.valueOf(i));
            }

            try {
                System.out.println("------sleep(500)-----");
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }

    }

    public static ShardedJedisSentinelPool init() {
        // 初始化连接池
        BeanFactory bf = new XmlBeanFactory(new ClassPathResource("applicationContext.xml"));
        ShardedJedisSentinelPool pool = bf.getBean("shardedJedisSentinelPool",
                ShardedJedisSentinelPool.class);

        System.out.println("init ShardedJedisSentinelPool success...");
        return pool;

    }

    public static void set(String key, String value) {
        ShardedJedis resource = null;
        boolean broken = false;
        try {
            resource = pool.getResource();
            resource.set(key, value);
        } catch (Exception e) {
            broken = true;
            e.printStackTrace();
        } finally {
            close(resource,broken);
        }
    }

    public static String get(String key) {
        ShardedJedis resource = null;
        String string = null;
        boolean broken = false;
        try {
            resource = pool.getResource();
            string = resource.get(key);
        } catch (Exception e) {
            broken = true;
            e.printStackTrace();
        } finally {
            close(resource,broken);
        }
        return string;
    }

    /**
     * 注意，此方法需要try-catch，因为当master发生变更后，监控线程会重新初始化连接池中的连接，造成抛错
     */
    public static void close(ShardedJedis resource, boolean broken) {
        if (null == resource) {
            return;
        }
        try {
            if(broken){
                pool.returnBrokenResource(resource);
            }else{
                pool.returnResource(resource);
            }
        } catch (Exception e) {
            resource.disconnect();
            e.printStackTrace();
        }
    }
}
