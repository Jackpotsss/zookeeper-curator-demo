package org.example.zk.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * ACL权限控制
 */
@Service
public class ACLService {

    public CuratorFramework getCuratorClientACL(boolean isStart) {
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")            //Zookeeper服务器地址，多个用逗号隔开
                .authorization("digest", "jack1:123456".getBytes())   //
                .sessionTimeoutMs(60000)                    //会话超时时间：默认60秒
                .connectionTimeoutMs(60000)            //连接超时时间：默认15秒
                .retryPolicy(new RetryForever(3000))   //设置重试策略
                .build();
        if(isStart){
            curator.start();//建立一个新会话
        }
        return curator;
    }

    private String getDigestUserPwd(String id) throws Exception {
        return DigestAuthenticationProvider.generateDigest(id); // 加密明文密码:先SHA1，再Base64
    }

    //创建 ACL 节点
    public void testCreateAclNode()throws Exception {

        String nodePath = "/test-curator/acl";
        String data = "acl-data";
        Id id1 = new Id("digest", getDigestUserPwd("jack1:123456"));
        Id id2 = new Id("digest", getDigestUserPwd("jack2:123456"));
        Id id3 = new Id("digest", getDigestUserPwd("jack3:123456"));
        List<ACL> aclList = Arrays.asList(
                new ACL(ZooDefs.Perms.ALL, id1), //id1 有所有权限：READ、WRITE、CREATE、DELETE、ADMIN
                new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, id2),
                new ACL(ZooDefs.Perms.READ, id3)
        );
        CuratorFramework curatorClientACL = getCuratorClientACL(true);
//        curatorClientACL.create().creatingParentsIfNeeded()
//                .withMode(CreateMode.PERSISTENT)
//                .withACL(aclList, true)  //
//                .forPath(nodePath, data.getBytes(StandardCharsets.UTF_8));
        //查询
        byte[] bytes = curatorClientACL.getData().forPath(nodePath);
        System.out.println(new String(bytes));
        //
        List<ACL> acls = queryNodeAcl(nodePath);
        acls.forEach(System.out::println);
    }

    /**
     * 查询节点设置的权限信息
     * @param nodePath
     * @throws Exception
     */
    public List<ACL> queryNodeAcl(String nodePath)throws Exception {
        CuratorFramework curatorClientACL = getCuratorClientACL(true);
        return curatorClientACL.getACL().forPath(nodePath);
    }

    /**
     * 为 ZNode 增加 ACL 权限控制
     * @param path
     * @throws Exception
     */
    public void updateAclNode(String path)throws Exception {
        CuratorFramework curatorClientACL = getCuratorClientACL(true);
        Id id1 = new Id("digest", getDigestUserPwd("jack1:123456"));
        Id id2 = new Id("digest", getDigestUserPwd("jack2:123456"));
        Id id3 = new Id("digest", getDigestUserPwd("jack3:123456"));
        List<ACL> aclList = Arrays.asList(
                new ACL(ZooDefs.Perms.ALL, id1), //id1 有所有权限：READ、WRITE、CREATE、DELETE、ADMIN
                new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, id2),
                new ACL(ZooDefs.Perms.READ, id3)
        );
        curatorClientACL.setACL().withACL(aclList).forPath(path);
    }
}
