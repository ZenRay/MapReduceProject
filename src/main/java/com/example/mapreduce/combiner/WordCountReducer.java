package com.example.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 计算数据
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        outValue.set(sum);
        // 写出数据
        context.write(key, outValue);
    }
}
