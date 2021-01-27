/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.mapreduce.wordcount.WordCountReducer
 * @date: 2020/11/19 13:34
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description: 在 Reduce 中进行单词出现次数的统计：
 * <br>
 * @date: 2020/11/19 13:34
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
