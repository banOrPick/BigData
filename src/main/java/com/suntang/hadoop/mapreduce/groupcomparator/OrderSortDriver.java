/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.mapreduce.groupcomparator.Or
 * @date: 2020/11/24 15:50
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.mapreduce.groupcomparator;
import com.suntang.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.suntang.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/24 15:50
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */



public class OrderSortDriver {

    @Test
    public  void OrderSort() throws Exception {
        // 1 获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar包加载路径
        job.setJarByClass(OrderSortDriver.class);

        // 3 加载map/reduce类
        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);

        // 4 设置map输出数据key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终输出数据的key和value类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path("F:\\input\\GroupingComparator.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\output"));


        // 10 设置reduce端的分组
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        // 7 设置分区
        job.setPartitionerClass(OrderSortPartitioner.class);

        // 8 设置reduce个数
        job.setNumReduceTasks(3);

        // 9 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
