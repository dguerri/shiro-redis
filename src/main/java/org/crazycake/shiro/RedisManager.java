package org.crazycake.shiro;

import java.util.ArrayList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisManager {

	private String host = "127.0.0.1";

	private int port = 6379;

	private String siblingHosts = null;

	// 0 - never expire
	private int expire = 0;

	//timeout for jedis try to connect to redis server, not expire time! In milliseconds
	private int timeout = 0;

	private String password = "";

	private static JedisPool jedisPool = null;
	private static ArrayList<JedisPool> siblingJedisPools = null;

	private static final Logger log = LoggerFactory.getLogger(RedisManager.class);

	public RedisManager() {

	}

	private JedisPool createJedisPool(String host, int port, int timeout, String password) {
		if (password != null && !"".equals(password)) {
			return new JedisPool(new JedisPoolConfig(), host, port, timeout, password);
		} else if (timeout != 0) {
			return new JedisPool(new JedisPoolConfig(), host, port, timeout);
		} else {
			return new JedisPool(new JedisPoolConfig(), host, port);
		}
	}

	/**
	 * 初始化方法
	 */
	public void init() {
		if (jedisPool == null) {
			jedisPool = createJedisPool(host, port, timeout, password);
		}
		if (siblingHosts != null && siblingJedisPools == null) {
			siblingJedisPools = new ArrayList();
			int siblingPort;
			String[] hp;
			for (String hostPort: siblingHosts.split("[,\\s]+")) {
				hp = hostPort.trim().split(":", 2);
				if (hp.length == 2) {
					siblingPort = Integer.parseInt(hp[1]);
				} else {
					siblingPort = port;
				}
				siblingJedisPools.add(createJedisPool(hp[0], siblingPort, timeout, password));
			}
		}
	}

	/**
	 * get value from redis
	 * @param key
	 * @return
	 */
	public byte[] get(byte[] key) {
		byte[] value = null;
		Jedis jedis = jedisPool.getResource();
		try{
			value = jedis.get(key);
		}finally{
			jedisPool.returnResource(jedis);
		}
		return value;
	}

	private void setInPool(JedisPool pool, byte[] key, byte[] value, int expire) throws redis.clients.jedis.exceptions.JedisConnectionException {
		Jedis jedis = pool.getResource();

		try {
			jedis.set(key, value);
			if (expire != 0) {
				jedis.expire(key, expire);
		 	}
		} finally {
			pool.returnResource(jedis);
		}
	}

	/**
	 * set
	 * @param key
	 * @param value
	 * @param expire
	 * @return
	 */
	public byte[] set(byte[] key,byte[] value, int expire) throws redis.clients.jedis.exceptions.JedisConnectionException {
		if (siblingJedisPools != null) {
			for (JedisPool pool: siblingJedisPools) {
				try {
					setInPool(pool, key, value, expire);
				} catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
					log.error("Cannot write to a sibling host: " + e);
				}
			}
		}

		setInPool(jedisPool, key, value, this.expire);
		return value;
	}

	/**
	 * set
	 * @param key
	 * @param value
	 * @return
	 */
	public byte[] set(byte[] key, byte[] value) throws redis.clients.jedis.exceptions.JedisConnectionException {
		return set(key, value, this.expire);
	}

	private void delFormPool(JedisPool pool, byte[] key) throws redis.clients.jedis.exceptions.JedisConnectionException {
		Jedis jedis = pool.getResource();
		try {
			jedis.del(key);
		} finally{
			pool.returnResource(jedis);
		}
	}

	/**
	 * del
	 * @param key
	 */
	public void del(byte[] key) throws redis.clients.jedis.exceptions.JedisConnectionException {
		if (siblingJedisPools != null) {
			for (JedisPool pool: siblingJedisPools) {
				try {
					delFormPool(pool, key);
				} catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
					log.error("Cannot delete from a sibling host: " + e);
				}
			}
		}

		delFormPool(jedisPool, key);
	}

	private void flushDBForPool(JedisPool pool) {
		Jedis jedis = pool.getResource();
		try {
			jedis.flushDB();
		} finally {
			pool.returnResource(jedis);
		}
	}

	/**
	 * flush
	 */
	public void flushDB() {
		flushDBForPool(jedisPool);

		if (siblingJedisPools != null) {
			for (JedisPool pool: siblingJedisPools) {
				flushDBForPool(pool);
			}
		}
	}

	/**
	 * size
	 */
	public Long dbSize() {
		Long dbSize = 0L;
		Jedis jedis = jedisPool.getResource();
		try{
			dbSize = jedis.dbSize();
		}finally{
			jedisPool.returnResource(jedis);
		}
		return dbSize;
	}

	/**
	 * keys
	 * @param regex
	 * @return
	 */
	public Set<byte[]> keys(String pattern) {
		Set<byte[]> keys = null;
		Jedis jedis = jedisPool.getResource();
		try{
			keys = jedis.keys(pattern.getBytes());
		}finally{
			jedisPool.returnResource(jedis);
		}
		return keys;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getSiblingHosts() {
		return siblingHosts;
	}

	public void setSiblingHosts(String hosts) {
		this.siblingHosts = hosts;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getExpire() {
		return expire;
	}

	public void setExpire(int expire) {
		this.expire = expire;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}



}
