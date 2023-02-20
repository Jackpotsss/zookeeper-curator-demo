package org.example.zk.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Curator主要实现了下面四种锁
 *
 * InterProcessMutex：分布式可重入排它锁
 * InterProcessSemaphoreMutex：分布式排它锁
 * InterProcessReadWriteLock：分布式读写锁
 *
 */
@Service
public class DistirbutedLockService {

    @Autowired
    private BaseCRUDService baseCRUDService;

    public void reentrantMutex(String path) {

        CuratorFramework curatorClient = baseCRUDService.getCuratorClient(true);
        // 创建分布式锁：可重入排他锁
        InterProcessLock lock1 = new InterProcessMutex(curatorClient, path);
        // 获取锁对象
        try {
            lock1.acquire();
            System.out.println(Thread.currentThread().getName()+" 获取锁");
            // 测试锁重入
            lock1.acquire();
            System.out.println(Thread.currentThread().getName()+" 再次获取锁");
            Thread.sleep(1000);
            lock1.release();
            System.out.println(Thread.currentThread().getName()+" 释放锁");
            lock1.release();
            System.out.println(Thread.currentThread().getName()+" 再次释放锁");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void semaphoreMutex(String path) {

        CuratorFramework curatorClient = baseCRUDService.getCuratorClient(true);
        // 创建分布式锁：排他锁
        InterProcessLock lock1 = new InterProcessSemaphoreMutex(curatorClient, path);
        // 获取锁对象
        try {
            lock1.acquire();
            System.out.println(Thread.currentThread().getName()+" 获取锁");
            Thread.sleep(1000);
            lock1.release();
            System.out.println(Thread.currentThread().getName()+" 释放锁");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void  testReadWriteLock(String path) {

        CuratorFramework curatorClient = baseCRUDService.getCuratorClient(true);
        // 创建分布式锁：读写锁
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(curatorClient, path);
        InterProcessReadWriteLock.ReadLock readLock = interProcessReadWriteLock.readLock();
//        InterProcessReadWriteLock.WriteLock writeLock = interProcessReadWriteLock.writeLock();
        // 获取锁对象
        try {
            readLock.acquire();
            System.out.println(Thread.currentThread().getName()+" 获取锁");
            Thread.sleep(1000);
            readLock.release();
            System.out.println(Thread.currentThread().getName()+" 释放锁");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
