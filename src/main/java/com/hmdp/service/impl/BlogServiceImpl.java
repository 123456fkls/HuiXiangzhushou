package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;
    
    // 博客缓存前缀
    private static final String CACHE_BLOG_KEY = "cache:blog:";

    @Override
    public Result queryBlogById(Long id) {
        // 使用二级缓存查询博客
        Blog blog = cacheClient.querryWithPassThrough(CACHE_BLOG_KEY, id, Blog.class, this::getById, 30L, TimeUnit.MINUTES);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        // 查询blog有关用户
        queryBlogUser(blog);
        // 查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 未登录,无需查询
            return;
        }
        Long userId = UserHolder.getUser().getId();
        //判断是否已经点过赞
        Double score = stringRedisTemplate.opsForZSet().score("blog:liked:" + blog.getId(), userId.toString());
        blog.setIsLike(score != null);

    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();
        //判断是否已经点过赞
        Double score = stringRedisTemplate.opsForZSet().score("blog:liked:" + id, userId.toString());
        if (score == null) {
            //未点赞
            //点赞数加1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            //保存用户到redis的set集合,sortedset (zadd key value score)
            if (isSuccess) {
                stringRedisTemplate.opsForZSet()
                        .add("blog:liked:" + id, userId.toString(), System.currentTimeMillis());
                // 清除本地缓存，确保下次查询时获取最新数据
                cacheClient.invalidate(CACHE_BLOG_KEY + id);
            }
        } else {
            //如果已点赞
            //点赞数减1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //从redis的set集合中移除
            stringRedisTemplate.opsForZSet()
                    .remove("blog:liked:" + id, userId.toString());
            // 清除本地缓存，确保下次查询时获取最新数据
            cacheClient.invalidate(CACHE_BLOG_KEY + id);
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 查询top5的点赞用户
        Set<String> top5 = stringRedisTemplate.opsForZSet()
                .range("blog:liked:" + id, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //解析出其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        //根据用户id查询用户
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("order by field(id," + idStr + ")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //返回用户
        return Result.ok(userDTOS);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}