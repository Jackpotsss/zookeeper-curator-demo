package org.example.zk.web;

import org.apache.curator.framework.CuratorFramework;
import org.example.zk.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.List;

@RestController
@RequestMapping("/test/zk")
public class TestController {


    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private BaseCRUDService baseCRUDService;
    @Autowired
    private WatcherService watcherService;
    @Autowired
    private NamespaceService namespaceService;
    @Autowired
    private ACLService aclService;
    @Autowired
    private DistirbutedLockService distirbutedLockService;

    private String path = "/test-curator/curator-1";

    @RequestMapping("/echo")
    public void sendFoo() {
        System.out.println("echo");
    }


    @RequestMapping("/createNode")
    public void createNode(@RequestParam String data,@RequestParam String path) throws Exception{
        CuratorFramework curatorFramework = baseCRUDService.createNode(path,data.getBytes(StandardCharsets.UTF_8));
        curatorFramework.close(); //关闭本次会话
    }
    @RequestMapping("/updateNode")
    public void updateNode(@RequestParam String data) throws Exception{

        CuratorFramework curatorFramework = baseCRUDService.updateNode(path,data.getBytes(StandardCharsets.UTF_8));
        curatorFramework.close();
    }

    @RequestMapping("/queryNode")
    public void queryNode(@RequestParam String path) throws Exception{
        CuratorFramework curatorFramework = baseCRUDService.queryNode(path);
        curatorFramework.close();
    }
    @RequestMapping("/queryNodeNamespace")
    public void queryNode(@RequestParam String namespace,@RequestParam String path) throws Exception{
        CuratorFramework curatorFramework = baseCRUDService.queryNode(path,namespace);
        curatorFramework.close();
    }

    @RequestMapping("/deleteNode")
    public void deleteNode(@RequestParam String path,@RequestParam Boolean containChildren) throws Exception{
        CuratorFramework curatorFramework = baseCRUDService.deleteNode(path,containChildren);
        curatorFramework.close();
    }
    @RequestMapping("/getChildrenNode")
    public List<String> getChildrenNode(@RequestParam String path) throws Exception{
        List<String> childrenNode = baseCRUDService.getChildrenNode(path);
        return childrenNode;
    }

    @RequestMapping("/listenerOnce")
    public void listenerOnce(@RequestParam String path) throws Exception{
        watcherService.listenerOnce(path);
    }
    @RequestMapping("/watcherCache")
    public void listenerOnce2(@RequestParam String path) throws Exception {
        watcherService.listenerOnce2(path);
    }
    @RequestMapping("/testNamespace")
    public void testNamespace() throws Exception {
        namespaceService.testNamespace();
    }
    @RequestMapping("/testCreateAclNode")
    public void testCreateAclNode() throws Exception {
        aclService.testCreateAclNode();
    }
    @RequestMapping("/testLock")
    public void testLock() throws Exception {
        distirbutedLockService.reentrantMutex("/test-curator/lock-1");
        distirbutedLockService.semaphoreMutex("/test-curator/lock-2");
    }
}
