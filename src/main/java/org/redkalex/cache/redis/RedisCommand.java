/*
 *
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;

/**
 *
 * @author zhangjx
 */
public enum RedisCommand {

    EXISTS("EXISTS", true),
    GET("GET", true),
    GETEX("GETEX", true),
    MSET("MSET", false),
    SET("SET", false),
    SETNX("SETNX", false),
    GETSET("GETSET", false),
    SETEX("SETEX", false),
    EXPIRE("EXPIRE", false),
    PERSIST("PERSIST", false),
    RENAME("RENAME", false),
    RENAMENX("RENAMENX", false),
    DEL("DEL", false),
    INCR("INCR", false),
    INCRBY("INCRBY", false),
    INCRBYFLOAT("INCRBYFLOAT", false),
    DECR("DECR", false),
    DECRBY("DECRBY", false),
    HDEL("HDEL", false),
    HINCRBY("HINCRBY", false),
    HINCRBYFLOAT("HINCRBYFLOAT", false),
    LINSERT("LINSERT", false),
    LTRIM("LTRIM", false),
    LPOP("LPOP", false),
    LPUSH("LPUSH", false),
    LPUSHX("LPUSHX", false),
    RPOP("RPOP", false),
    RPOPLPUSH("RPOPLPUSH", false),
    SMOVE("SMOVE", false),
    RPUSH("RPUSH", false),
    RPUSHX("RPUSHX", false),
    LREM("LREM", false),
    SADD("SADD", false),
    SPOP("SPOP", false),
    SSCAN("SSCAN", true),
    SREM("SREM", false),
    ZADD("ZADD", false),
    ZINCRBY("ZINCRBY", false),
    ZREM("ZREM", false),
    ZMSCORE("ZMSCORE", true),
    ZSCORE("ZSCORE", true),
    ZCARD("ZCARD", true),
    ZRANK("ZRANK", true),
    ZREVRANK("ZREVRANK", true),
    ZRANGE("ZRANGE", true),
    ZSCAN("ZSCAN", true),
    FLUSHDB("FLUSHDB", false),
    FLUSHALL("FLUSHALL", false),
    HMGET("HMGET", true),
    HLEN("HLEN", true),
    HKEYS("HKEYS", true),
    HEXISTS("HEXISTS", true),
    HSET("HSET", true),
    HSETNX("HSETNX", true),
    HMSET("HMSET", true),
    HSCAN("HSCAN", true),
    HGETALL("HGETALL", true),
    HVALS("HVALS", true),
    HGET("HGET", true),
    HSTRLEN("HSTRLEN", true),
    SMEMBERS("SMEMBERS", true),
    SISMEMBER("SISMEMBER", true),
    LRANGE("LRANGE", true),
    LINDEX("LINDEX", true),
    LLEN("LLEN", true),
    SCARD("SCARD", true),
    SMISMEMBER("SMISMEMBER", true),
    SRANDMEMBER("SRANDMEMBER", true),
    SDIFF("SDIFF", true),
    SDIFFSTORE("SDIFFSTORE", false),
    SINTER("SINTER", true),
    SINTERSTORE("SINTERSTORE", false),
    SUNION("SUNION", true),
    SUNIONSTORE("SUNIONSTORE", false),
    MGET("MGET", true),
    KEYS("KEYS", true),
    SCAN("SCAN", true),
    DBSIZE("DBSIZE", true),
    TYPE("TYPE", true);

    private final String command;

    private final byte[] bytes;

    private final boolean readOnly;

    private RedisCommand(String command, boolean readOnly) {
        this.command = command;
        this.readOnly = readOnly;
        this.bytes = ("$" + command.length() + "\r\n" + command + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
    }

    public String getCommand() {
        return command;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}
