/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.mapreduce.groupcomparator.OrderSortGroupingComparator
 * @date: 2020/11/24 15:51
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.mapreduce.groupcomparator;

import com.suntang.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/24 15:51
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class OrderSortPartitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean key, NullWritable value, int numReduceTasks) {

        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
