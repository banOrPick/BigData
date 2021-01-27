/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.mapreduce.groupcomparator.OrderSortReduce
 * @date: 2020/11/24 15:50
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.mapreduce.groupcomparator;
import java.io.IOException;

import com.banorpick.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/24 15:50
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */


public class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
    @Override
    protected void reduce(OrderBean bean, Iterable<NullWritable> values,
        Context context) throws IOException, InterruptedException {
        // 直接写出
        context.write(bean, NullWritable.get());
    }
}
