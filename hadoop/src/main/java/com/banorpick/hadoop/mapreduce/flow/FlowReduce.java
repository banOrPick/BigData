/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.mapreduce.flow.FlowReduce
 * @date: 2020/11/19 17:07
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.mapreduce.flow;

import com.banorpick.hadoop.mapreduce.bean.Flowbean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/19 17:07
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class FlowReduce extends Reducer<Text, Flowbean, Text, Flowbean> {

    @Override
    protected void reduce(Text key, Iterable<Flowbean> values, Context context)
        throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long downUpFlow = 0;
        for (Flowbean bean : values) {
            sumUpFlow += bean.getUpFlow();
            downUpFlow += bean.getDownFlow();
        }
        context.write(key, new Flowbean(sumUpFlow, downUpFlow));
    }
}
