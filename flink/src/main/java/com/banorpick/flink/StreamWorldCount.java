package com.banorpick.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamWorldCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);

        String inputPath = "F:\\develop\\hjrProject\\BigData\\flink\\src\\main\\resources\\hello.txt";

        DataStream<String> dataStream = env.readTextFile(inputPath);

        DataStream<Tuple2<String, Integer>> wordCountStream = dataStream
                .flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountStream.print();

        env.execute();
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
