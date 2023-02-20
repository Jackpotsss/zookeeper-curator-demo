package org.example.zk.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 提供Curator客户端对zk服务器的基本增删改查
 * 重试策略：
 * RetryForever：一直重试，可设置重试间隔
 * ExponentialBackoffRetry：设置初次重试间隔和总重试次数
 */
@Service
public class BaseCRUDService {

    public CuratorFramework getCuratorClient(boolean isStart) {
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")            //Zookeeper服务器地址，多个用逗号隔开
                .sessionTimeoutMs(60000)                    //会话超时时间：默认60秒
                .connectionTimeoutMs(60000)            //连接超时时间：默认15秒
                .retryPolicy(new RetryForever(3000))   //设置重试策略
                .build();
        if(isStart){
            curator.start();//建立一个新会话
        }
        return curator;
    }

    public CuratorFramework getCuratorClient(boolean isStart,String namespace) {
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")            //Zookeeper服务器地址，多个用逗号隔开
                .sessionTimeoutMs(60000)                    //会话超时时间：默认60秒
                .connectionTimeoutMs(60000)            //连接超时时间：默认15秒
                .retryPolicy(new RetryForever(3000))   //设置重试策略
                .namespace(namespace)       //命名空间、隔离业务
                .build();
        if(isStart){
            curator.start();//建立一个新会话
        }
        return curator;
    }
    public CuratorFramework createNode(String path,byte[] data) throws Exception {

        CuratorFramework curatorClient = getCuratorClient(true,"test-curator/levle2-01");
        //创建节点
        curatorClient.create()
                .creatingParentsIfNeeded()      // 如果父节点不存在则自动创建
                .withMode(CreateMode.PERSISTENT)     //设置节点类型，默认为持久化节点
                .forPath(path, data);
        return curatorClient;
    }
    public CuratorFramework queryNode(String namespace,String path) throws Exception {

        CuratorFramework curatorClient = getCuratorClient(true,namespace);
        byte[] bytes = curatorClient.getData().forPath(path);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        return curatorClient;
    }
    //查询节点数据
    public CuratorFramework queryNode(String path) throws Exception {
        //普通查询
        CuratorFramework curatorClient = getCuratorClient(true);
        byte[] bytes = curatorClient.getData().forPath(path);
        String namespace = curatorClient.getNamespace();
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        /**
         * 包含状态的查询:Stat
         * czxid  创建节点时的事务Id
         * mzxid    修改节点时的事务 id
         * ctime    节点创建时的毫秒值
         * mtime    节点修改时的毫秒值
         * version  节点版本（数据修改的次数）
         * cversion 子节点修改的次数
         * aversion ACL修改的次数
         * ephemeralOwner   如果是临时节点，该值为节点的 SessionId，其它类型的节点则为 0
         * dataLength   数据长度
         * numChildren  子节点数量
         * pzxid        添加和删除子节点的事务 id
         */
        Stat stat = new Stat();
        curatorClient.getData().storingStatIn(stat).forPath(path);
        System.out.println(stat);
        return curatorClient;
    }

    public List<String> getChildrenNode(String path) throws Exception {
        CuratorFramework curatorClient = getCuratorClient(true);
        List<String> children = curatorClient.getChildren().forPath(path);//查询某节点的所有直接子节点
        return children;
    }
    public CuratorFramework updateNode(String path,byte[] data) throws Exception {

        CuratorFramework curatorClient = getCuratorClient(true);
        // 普通更新
        curatorClient.setData().forPath(path,data);
//        // 指定版本更新
//        curatorClient.setData().withVersion(1).forPath(path);
        return curatorClient;
    }
    public CuratorFramework deleteNode(String path,boolean containChildren) throws Exception {
        CuratorFramework curatorClient = getCuratorClient(true);
        // 递归删除子节点
        if(containChildren){
            curatorClient.delete().deletingChildrenIfNeeded().forPath(path);
        }else{
            // 普通删除
            curatorClient.delete().forPath(path);
        }
        // 指定版本删除
//        curatorClient.delete().withVersion(1).forPath(path);
        // 强制删除
//        curatorClient.delete().guaranteed().forPath(path);
        return curatorClient;
    }

}