/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.mapreduce.bean.Flowbean
 * @date: 2020/11/19 16:51
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.mapreduce.bean;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/19 16:51
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
@Data
public class Flowbean implements Writable {

    private long upFlow;

    private long downFlow;

    private long sunFlow;

    public Flowbean() {
        super();
    }

    public Flowbean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sunFlow = upFlow + downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sunFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.sunFlow=in.readLong();
    }
}
