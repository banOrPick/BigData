/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.mapreduce.flow.FlowApp
 * @date: 2020/11/19 17:13
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.mapreduce.flow;

import com.suntang.hadoop.mapreduce.bean.Flowbean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.net.URI;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/19 17:13
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class FlowApp {

    // 这里为了直观显示参数 使用了硬编码，实际开发中可以通过外部传参
    private static final String HDFS_URL = "hdfs://192.168.1.102:8020";
    private static final String HADOOP_USER_NAME = "root";
    @Test
    public  void flowApp() throws Exception {

        //  文件输入路径和输出路径由外部传参指定
        //        if (args.length < 2) {
        //            System.out.println("Input and output paths are necessary!");
        //            return;
        //        }

        // 需要指明 hadoop 用户名，否则在 HDFS 上创建目录时可能会抛出权限不足的异常
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);

        Configuration configuration = new Configuration();
        // 指明 HDFS 的地址
        configuration.set("fs.defaultFS", HDFS_URL);

        // 创建一个 Job
        Job job = Job.getInstance(configuration);

        // 设置运行的主类
        job.setJarByClass(FlowApp.class);

        // 设置 Mapper 和 Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        // 设置 Mapper 输出 key 和 value 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flowbean.class);

        // 设置 Reducer 输出 key 和 value 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        // 如果输出目录已经存在，则必须先删除，否则重复运行程序时会抛出异常
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER_NAME);
        Path outputPath = new Path("/wordcount/flow/flowApp");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        // 设置作业输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, outputPath);

        // 将作业提交到群集并等待它完成，参数设置为 true 代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);

        // 关闭之前创建的 fileSystem
        fileSystem.close();

        // 根据作业结果,终止当前运行的 Java 虚拟机,退出程序
        System.exit(result ? 0 : -1);

    }
}
