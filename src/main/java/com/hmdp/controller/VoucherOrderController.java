package com.hmdp.controller;


import com.hmdp.annotation.RateLimit;
import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService vouncherService;

    @PostMapping("seckill/{id}")
    @RateLimit(type = RateLimit.LimitType.USER, window = 60, count = 5, message = "秒杀过于频繁，请稍后再试")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) throws InterruptedException {
        return vouncherService.seckillVoucher(voucherId);
    }
}