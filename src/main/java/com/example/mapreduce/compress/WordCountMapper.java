package com.example.mapreduce.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取每行的数据
        String line = value.toString();

        // 2. 分割数据
        String[] words = line.split(" ");

        // 3. 循环抛出数据
        for (String word : words) {
            outKey.set(word);

            context.write(outKey, outValue);
        }


    }
}
