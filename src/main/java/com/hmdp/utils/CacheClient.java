package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;


import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 本地缓存
    private final Cache<String, Object> localCache = Caffeine.newBuilder()
            .initialCapacity(100)
            .maximumSize(1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
        // 同时更新本地缓存
        localCache.put(key, value);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
        // 同时更新本地缓存
        localCache.put(key, value);
    }

    public <R, ID> R querryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从本地缓存查
        R localValue = (R) localCache.getIfPresent(key);
        if (localValue != null) {
            return localValue;
        }
        //2.从 redis 查缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        //3.判断是否存在
        if (StrUtil.isNotBlank(Json)) {
            //4.存在，写入本地缓存并返回
            R value = JSONUtil.toBean(Json, type);
            localCache.put(key, value);
            return value;
        }
        //判断是否为空值（缓存穿透）
        if ("".equals(Json)) {
            return null;
        }
        //5.不存在，查数据库
        R r = dbFallback.apply(id);
        //6.不存在，将空值写入 redis
        if (r == null) {
            //将空值返回 redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //7.数据库存在，先写入 redis，再写入本地缓存，最后返回
        this.set(key, r, time, unit);
        localCache.put(key, r);
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R querryWithLogicalExpire(String keyPrefix,ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从本地缓存查
        R localValue = (R) localCache.getIfPresent(key);
        if (localValue != null) {
            return localValue;
        }
        //2.从 redis 查缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //3.判断是否存在
        if (StrUtil.isBlank(json)) {
            //4.不命中，直接返回
            return null;
        }
        //5.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //6.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //6.1.未过期，写入本地缓存并返回
            localCache.put(key, r);
            return r;
        }
        //6.2.已过期，需要缓存重建
        //7.缓存重建
        //7.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //7.2.判断是否获取成功
        if (isLock) {
            //7.3.成功，开启独立线程，缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    // 获取数据库数据
                    R r1 = dbFallback.apply(id);
                    //  写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                    // 写入本地缓存
                    localCache.put(key, r1);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //7.4.返回过期的数据
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    // 清除本地缓存
    public void invalidate(String key) {
        localCache.invalidate(key);
    }

}