/*
 * Project Name: hadooptest
 * Class Name: com.suntang.hadoop.mapreduce.groupcomparator.orderBean
 * @date: 2020/11/24 15:42
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.suntang.hadoop.mapreduce.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/24 15:42
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;

    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(String orderId, double price) {
        super();
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public int compareTo(OrderBean o) {
        // 1 先按订单id排序(从小到大)
        int result = this.orderId.compareTo(o.getOrderId());

        if (result == 0) {
            // 2 再按金额排序（从大到小）
            result = price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
