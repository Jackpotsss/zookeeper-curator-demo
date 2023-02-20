package org.example.zk.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * 命名空间下操作
 */
@Service
public class NamespaceService {


    public CuratorFramework getCuratorClient(boolean isStart, String namespace) {
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

    public void testNamespace() throws Exception {
        String namespace = "test-curator/level2-namespace";   //注意前缀不能为 /
        String path = "/level3-namespace";
        CuratorFramework curatorClient = getCuratorClient(true, namespace);

        //创建节点
        curatorClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, "level3-namespace-data".getBytes(StandardCharsets.UTF_8));
        //查询数据
        byte[] bytes = curatorClient.getData().forPath(path);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        //删除节点
//        curatorClient.delete().forPath(path);
    }

}
