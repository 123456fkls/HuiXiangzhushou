package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public List<ShopType> queryShopType() {
        // 从 redis 查询所有
        String shopTypeJson = stringRedisTemplate.opsForValue().get("cache:shop:type");
        // 判断是否存在
        if (StrUtil.isNotBlank(shopTypeJson)) {
            // 存在，直接返回
            return JSONUtil.toList(shopTypeJson, ShopType.class);
        }
        // 不存在，从数据库查询
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        // 数据库存在，写入 redis 并设置过期时间
        if (shopTypes != null) {
            stringRedisTemplate.opsForValue().set("cache:shop:type", JSONUtil.toJsonStr(shopTypes), 30, TimeUnit.MINUTES);
        }
        // 数据库不存在，返回空列表
        return shopTypes != null ? shopTypes : null;
    }
}
