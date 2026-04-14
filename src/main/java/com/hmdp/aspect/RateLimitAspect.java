package com.hmdp.aspect;

import com.hmdp.annotation.RateLimit;
import com.hmdp.dto.Result;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * 限流切面
 */
@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Pointcut("@annotation(com.hmdp.annotation.RateLimit)")
    public void rateLimitPointcut() {}

    @Around("rateLimitPointcut()")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        RateLimit rateLimit = method.getAnnotation(RateLimit.class);

        // 获取限流key
        String key = generateKey(rateLimit);
        // 获取时间窗口和最大请求数
        int window = rateLimit.window();
        TimeUnit unit = rateLimit.unit();
        int count = rateLimit.count();
        String message = rateLimit.message();

        // 执行限流逻辑
        if (!tryAcquire(key, window, unit, count)) {
            return Result.fail(message);
        }

        // 执行原方法
        return joinPoint.proceed();
    }

    /**
     * 生成限流key
     */
    private String generateKey(RateLimit rateLimit) {
        StringBuilder key = new StringBuilder(rateLimit.prefix());

        switch (rateLimit.type()) {
            case IP:
                // 获取客户端IP
                HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
                String ip = request.getRemoteAddr();
                key.append(":ip:").append(ip);
                break;
            case USER:
                // 获取当前用户
                if (UserHolder.getUser() != null) {
                    key.append(":user:").append(UserHolder.getUser().getId());
                } else {
                    // 未登录用户使用IP
                    HttpServletRequest req = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
                    String ipAddr = req.getRemoteAddr();
                    key.append(":ip:").append(ipAddr);
                }
                break;
            case GLOBAL:
                // 全局限流，不需要额外标识
                break;
        }

        return key.toString();
    }

    /**
     * 尝试获取限流令牌
     */
    private boolean tryAcquire(String key, int window, TimeUnit unit, int count) {
        // 转换时间窗口为毫秒
        long windowMillis = unit.toMillis(window);
        // 获取当前时间戳
        long currentTime = Instant.now().toEpochMilli();
        // 计算时间窗口的起始时间
        long startTime = currentTime - windowMillis;

        // 使用Redis的ZSet实现滑动窗口
        // 1. 移除时间窗口外的记录
        stringRedisTemplate.opsForZSet().removeRangeByScore(key, 0, startTime);
        // 2. 获取当前窗口内的请求数
        Long currentCount = stringRedisTemplate.opsForZSet().zCard(key);
        // 3. 判断是否超过限制
        if (currentCount >= count) {
            return false;
        }
        // 4. 添加当前请求到时间窗口
        stringRedisTemplate.opsForZSet().add(key, String.valueOf(currentTime), currentTime);
        // 5. 设置过期时间，避免内存泄漏
        stringRedisTemplate.expire(key, window, unit);

        return true;
    }
}