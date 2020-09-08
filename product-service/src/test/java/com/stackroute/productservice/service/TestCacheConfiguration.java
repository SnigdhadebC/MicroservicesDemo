package com.stackroute.productservice.service;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheFactoryBean;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a configuration class for caching
 */
@TestConfiguration
@EnableCaching
public class TestCacheConfiguration {

    @Bean
    public SimpleCacheManager cacheManager() {
        SimpleCacheManager cacheManager = new SimpleCacheManager();
        List<Cache> caches = new ArrayList<>();
        caches.add(productsCacheBean().getObject());
        caches.add(categoryProductsCacheBean().getObject());
        cacheManager.setCaches(caches);
        return cacheManager;
    }

    @Bean
    public ConcurrentMapCacheFactoryBean productsCacheBean() {
        ConcurrentMapCacheFactoryBean cacheFactoryBean = new ConcurrentMapCacheFactoryBean();
        cacheFactoryBean.setName("products");
        return cacheFactoryBean;
    }

    @Bean
    public ConcurrentMapCacheFactoryBean categoryProductsCacheBean() {
        ConcurrentMapCacheFactoryBean cacheFactoryBean = new ConcurrentMapCacheFactoryBean();
        cacheFactoryBean.setName("categoryProducts");
        return cacheFactoryBean;
    }

}