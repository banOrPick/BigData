/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.mapreduce.flow.FlowPartitioner
 * @date: 2020/11/23 18:34
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.mapreduce.flow;

import com.suntang.hadoop.mapreduce.bean.Flowbean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/23 18:34
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class FlowPartitioner extends Partitioner<Text, Flowbean> {
    @Override
    public int getPartition(Text key, Flowbean value, int numPartitions) {
        String phoneNum = key.toString().substring(0, 3);
        int result=0;
        if("135".equals(phoneNum)){
            result=1;
        }
        if("136".equals(phoneNum)){
            result=2;
        }
        if("137".equals(phoneNum)){
            result=3;
        }
        if("138".equals(phoneNum)){
            result=4;
        }
        return result;
    }
}
