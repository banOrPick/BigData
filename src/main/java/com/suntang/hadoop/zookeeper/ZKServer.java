/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.zookeeper.ZKServer
 * @date: 2020/11/25 15:08
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.zookeeper;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/25 15:08
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class ZKServer {

    //连接zk服务器的地址及端口
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181,hadoop105:2181";

    //超时时间
    private int timeOut = 2000;

    private ZooKeeper zk = null;

    private String parentNode = "/servers";

    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
            }
        });
    }

    /**
     * 注册
     *
     * @param hostName
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void regist(String hostName) throws KeeperException, InterruptedException {
        String result = zk.create(parentNode + "/server",
            hostName.getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostName + " is online " + result);
    }

    /**
     * 业务逻辑
     */
    public void business() throws InterruptedException {
        System.out.println("server业务逻辑");
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void start() throws Exception {
        getConnect();

        regist("hadoop102");

        business();
    }

}
