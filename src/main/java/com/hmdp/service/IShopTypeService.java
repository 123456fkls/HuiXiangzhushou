package com.hmdp.service;

import com.hmdp.entity.ShopType;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

public interface IShopTypeService extends IService<ShopType> {

    List<ShopType> queryShopType();
}
