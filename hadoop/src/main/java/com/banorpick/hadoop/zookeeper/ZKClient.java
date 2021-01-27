/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.zookeeper.ZKClient
 * @date: 2020/11/25 14:11
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.zookeeper;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/25 14:11
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class ZKClient {

    //连接zk服务器的地址及端口
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181,hadoop105:2181";

    //超时时间
    private int timeOut = 2000;

    private ZooKeeper zkCli = null;

    private String parentNode = "/servers";

    @Before
    public void initZk() throws IOException {
        zkCli = new ZooKeeper(connectString, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
                try {
                    getServers();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 监听节点变化
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getServers() throws Exception {
        List<String> children = zkCli.getChildren(parentNode, true);
        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zkCli.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        //打印数据
        System.out.println(servers);
    }

    /**
     * 业务逻辑
     */
    public void business() throws InterruptedException {
        System.out.println("client业务逻辑");
        Thread.sleep(Long.MAX_VALUE);
    }


    @Test
    public void listenTest() throws Exception {
        getServers();

        business();
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {
        //        zkCli.create("/root","testZk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkCli.create("/servers", "testServers".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/", false);
        children.forEach(p -> {
            System.out.println(p);
        });
    }

    @Test
    public void isExit() throws KeeperException, InterruptedException {
        Stat stat = zkCli.exists("/", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
