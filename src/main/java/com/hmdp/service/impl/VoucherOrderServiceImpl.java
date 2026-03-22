package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.*;


@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {


    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RedisIdWorker redisIdWorker;

    private static final DefaultRedisScript<Long> SEC_SCRIPT;

    static {
        SEC_SCRIPT = new DefaultRedisScript<>();
        SEC_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SEC_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                //获取队列中的订单信息
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    //创建订单
                    handlerVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    private void handlerVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        log.info("开始处理订单，userId: {}, voucherId: {}", userId, voucherOrder.getVoucherId());
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean islock = false;
        islock = lock.tryLock();
        //判断获取锁成功
        if (!islock) {
            //获取锁失败
            log.error("不允许重复下单，userId: {}", userId);
            return;
        }
        try {
            log.info("获取锁成功，开始创建订单，userId: {}", userId);
            // 直接调用本类的事务方法
            createVoucherOrder(voucherOrder);
            log.info("订单创建成功，userId: {}, orderId: {}", userId, voucherOrder.getId());
        } catch (Exception e) {
            log.error("创建订单失败，userId: {}, voucherId: {}", userId, voucherOrder.getVoucherId(), e);
            throw e;
        } finally {
            //释放锁
            lock.unlock();
            log.info("释放锁，userId: {}", userId);
        }
    }



    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SEC_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        //2.判断结果是否为0
        if (result.intValue() != 0) {
            //2.1 不为0，代表没有购买资格
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        //2.2 为0，代表购买资格，把下单信息保存到阻塞队列中

        VoucherOrder voucherOrder = new VoucherOrder();
        // 1.生成订单 id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.用户 id
        voucherOrder.setUserId(userId);
        //3.代金券 id
        voucherOrder.setVoucherId(voucherId);
        //4.放入阻塞队列
        orderTasks.add(voucherOrder);
        //3.返回订单 id
        return Result.ok(orderId);
    }


//    @Override
//    public Result seckillVoucher(Long voucherId)  {
//        //查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀是否开启
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//        //判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        //判断库存是否充足
//        if (voucher.getStock() < 1) {
//            //库存不足
//            return Result.fail("库存不足！");
//        }

//    Long userId = UserHolder.getUser().getId();
//    //创建锁对象

    /// /        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//    RLock lock = redissonClient.getLock("lock:order:" + userId);
//    //获取锁
//    boolean islock = lock.tryLock();
//    //判断获取锁成功
//        if(!islock)
//
//    {
//        //获取锁失败
//        return Result.fail("不允许重复下单");
//    }
//        try
//
//    {
//        //获取代理对象（事务）
//        IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return proxy.createVoucherOrder(voucherId);
//    } finally
//
//    {
//        //释放锁
//        lock.unlock();
//    }
//}
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        log.info("开始执行 createVoucherOrder, voucherId: {}, userId: {}", voucherOrder.getVoucherId(), voucherOrder.getUserId());
        //一人一单
        Long userId = voucherOrder.getUserId();
        //查询订单表中是否存在
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        log.info("查询订单是否存在，userId: {}, count: {}", userId, count);
        if (count > 0) {
            //返回（"失败，已经购买过"）
            log.error("用户已经购买过一次！userId: {}, voucherId: {}", userId, voucherOrder.getVoucherId());
            return;
        }
        //不存在，创建
        //扣减库存
        log.info("开始扣减库存，voucherId: {}", voucherOrder.getVoucherId());
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        log.info("库存扣减结果：{}", success);
        if (!success) {
            //扣减失败
            log.error("库存不足！voucherId: {}", voucherOrder.getVoucherId());
            return;
        }
        //创建订单
        log.info("开始保存订单，orderId: {}", voucherOrder.getId());
        save(voucherOrder);
        log.info("订单保存成功，orderId: {}", voucherOrder.getId());
            
        //将订单信息保存到 Redis 缓存
        String orderKey = "order:" + voucherOrder.getId();
        log.info("开始保存订单到 Redis, key: {}", orderKey);
        stringRedisTemplate.opsForValue().set(
            orderKey,
            JSONUtil.toJsonStr(voucherOrder),
            30,
            TimeUnit.DAYS
        );
        log.info("订单保存到 Redis 成功，key: {}", orderKey);
    }
}
