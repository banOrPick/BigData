/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.hdfs.AutoRack
 * @date: 2020/11/17 17:55
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.hdfs;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/17 17:55
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class AutoRack implements DNSToSwitchMapping {
    @Override
    public List<String> resolve(List<String> ips) {
        List<String> lists = new ArrayList<>();


        if (!CollectionUtils.isEmpty(ips)) {
            //1.获取机架ip
            Integer ipnumber = 0;
            for (String ip : ips) {
                if (ip.startsWith("hadoop")) {
                    String ipnum = ip.substring(6);
                    ipnumber = Integer.valueOf(ipnum);
                }
                else if (ip.startsWith("192")) {
                    int lastIndex = ip.lastIndexOf(".");
                    String ipnum = ip.substring(lastIndex + 1);
                    ipnumber = Integer.valueOf(ipnum);
                }
                //2.自定义机架(102,103定义为机架1，104,105定义为机架2)
                if (ipnumber < 104) {
                    lists.add("/rack1/" + ipnumber);
                }
                else {
                    lists.add("/rack2/" + ipnumber);
                }
            }
        }
        return lists;
    }

    @Override
    public void reloadCachedMappings() {

    }

    @Override
    public void reloadCachedMappings(List<String> list) {

    }
}
