package com.hmdp.annotation;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * 限流注解
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimit {
    /**
     * 限流key前缀
     */
    String prefix() default "rate:limit";

    /**
     * 限流维度
     */
    LimitType type() default LimitType.GLOBAL;

    /**
     * 时间窗口大小
     */
    int window() default 60;

    /**
     * 时间单位
     */
    TimeUnit unit() default TimeUnit.SECONDS;

    /**
     * 窗口内最大请求数
     */
    int count() default 100;

    /**
     * 限流提示信息
     */
    String message() default "系统繁忙，请稍后再试";

    /**
     * 限流维度枚举
     */
    enum LimitType {
        /**
         * 全局限流
         */
        GLOBAL,
        /**
         * IP限流
         */
        IP,
        /**
         * 用户限流
         */
        USER
    }
}