/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.mapreduce.wordcount.WordCountMapper
 * @date: 2020/11/19 11:47
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: KEYIN : mapping 输入 key 的类型，即每行的偏移量 (每行第一个字符在整个文本中的位置)，Long 类型，对应 Hadoop 中的 LongWritable 类型；
 * VALUEIN : mapping 输入 value 的类型，即每行数据；String 类型，对应 Hadoop 中 Text 类型；
 * KEYOUT ：mapping 输出的 key 的类型，即每个单词；String 类型，对应 Hadoop 中 Text 类型；
 * VALUEOUT：mapping 输出 value 的类型，即每个单词出现的次数；这里用 int 类型，对应 IntWritable 类型。
 * <br>
 *     将每行数据按照指定分隔符进行拆分
 * @date: 2020/11/19 11:47
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }

}
