package com.banorpick.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WorldCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "F:\\develop\\hjrProject\\BigData\\flink\\src\\main\\resources\\hello.txt";

        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> wordCountDataSet = inputDataSet
                .flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        wordCountDataSet.print();
    }


    private static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
