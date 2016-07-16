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

        BeanFactory bf = new XmlBeanFactory(new ClassPathResource("applicationContext.xml"));
        ShardedJedisSentinelPool pool = bf.getBean("shardedJedisSentinelPool",
                ShardedJedisSentinelPool.class);

        System.out.println("init ShardedJedisSentinelPool success...");
        return pool;

    }

    public static void set(String key, String value) {
        try {
            ShardedJedis resource = pool.getResource();
            resource.set(key, value);
            resource.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String get(String key) {
        ShardedJedis resource = null;
        String string = null;
        try {
            resource = pool.getResource();
            string = resource.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            resource.close();
        }
        return string;
    }
}
