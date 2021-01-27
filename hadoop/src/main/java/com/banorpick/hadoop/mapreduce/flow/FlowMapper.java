/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.mapreduce.flow.FlowMapper
 * @date: 2020/11/19 16:57
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.mapreduce.flow;

import com.banorpick.hadoop.mapreduce.bean.Flowbean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/19 16:57
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, Flowbean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        String phoneNum = words[1];
        long upFlow = Long.parseLong(words[words.length - 3]);
        long downFlow = Long.parseLong(words[words.length - 2]);
        context.write(new Text(phoneNum), new Flowbean(upFlow, downFlow));
    }
}
