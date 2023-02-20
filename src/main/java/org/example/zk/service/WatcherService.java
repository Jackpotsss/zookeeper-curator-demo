package org.example.zk.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.CuratorCacheListenerBuilder;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class WatcherService {

    @Autowired
    private BaseCRUDService baseCRUDService;

    public void listenerOnce(String path) throws Exception {

        CuratorFramework curatorClient = baseCRUDService.getCuratorClient(true);
        CuratorWatcher curatorWatcher = watchedEvent -> {
            System.out.println(watchedEvent);
        };
        // 获取节点的 data 数据，并对节点开启监听
        curatorClient.getData().usingWatcher(curatorWatcher).forPath(path);
    }

    public void listenerOnce2(String path) throws Exception {

        CuratorFramework curatorClient = baseCRUDService.getCuratorClient(true);
        //此选项只缓存给定的节点（即单个节点缓存）
        CuratorCache curatorCache = CuratorCache.build(curatorClient, path);
        CuratorCacheListener curatorCacheListener = CuratorCacheListener.builder()
                .forInitialized(() -> {  // 初始化完成时调用
                    System.out.println("forInitialized");
                }).forCreatesAndChanges((oldNode, node) -> {
                    System.out.printf("[forCreatesAndChanges] : Node changed: Old: [%s] New: [%s]\n", oldNode, node);
                }).forCreates(childData -> {
                    System.out.printf("[forCreates] : Node created: [%s]\n", childData);
                } ).forChanges((oldNode, node) -> {
                    System.out.printf("[forChanges] : Node changed: Old: [%s] New: [%s]\n", oldNode, node);
                }).forDeletes(childData -> {
                    System.out.printf("[forDeletes] : Node delete: Old: [%s] New: [%s]\n", childData);
                }).forAll((type, oldData, data) -> { //新建、删除、变更
                    System.out.printf("[forAll] : type: [%s] [%s] [%s]\n", type, oldData, data);
                }).forNodeCache(()->{    //基于cache实现监听  NodeCache  PathCache  TreeCache
                    System.out.println("forNodeCache");
                }).forPathChildrenCache(path,curatorClient,(client,event)->{
                    System.out.println("forPathChildrenCache");
                }).forTreeCache(curatorClient,(client,event)->{
                    System.out.println("forTreeCache");
                }).build();
        curatorCache.listenable().addListener(curatorCacheListener);       // 给CuratorCache实例添加监听器
        curatorCache.start();   // 启动CuratorCache

    }
}
