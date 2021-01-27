/*
 * Project Name: hadooptest
 * Class Name: com.banorpick.hadoop.mapreduce.groupcomparator.OrderSortGroupingComparator
 * @date: 2020/11/24 15:53
 *
 * Copyright (C) 2019,suntang.com All Right Reserved.
 */
package com.banorpick.hadoop.mapreduce.groupcomparator;

import com.banorpick.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @description: // TODO
 * <br>
 * @date: 2020/11/24 15:53
 * @author: h00000042/huJinRui
 * @version: PoliceAnalysis V1.0
 * @since: JDK 1.8
 */

public class OrderSortGroupingComparator extends WritableComparator {

    protected OrderSortGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean abean = (OrderBean)a;
        OrderBean bbean = (OrderBean)b;

        // 将orderId相同的bean都视为一组
        return abean.getOrderId().compareTo(bbean.getOrderId());
    }
}

