/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.mapreduce.groupcomparator.OrderSortMapper
 * @date: 2020/11/24 15:49
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.mapreduce.groupcomparator;

import java.io.IOException;

import com.suntang.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/24 15:49
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */

public class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();

        // 2 截取字段
        String[] fields = line.split("\t");

        // 3 封装bean
        bean.setOrderId(fields[0]);
        bean.setPrice(Double.parseDouble(fields[2]));

        // 4 写出
        context.write(bean, NullWritable.get());
    }
}
