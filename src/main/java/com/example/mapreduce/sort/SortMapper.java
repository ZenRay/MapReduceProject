package com.example.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, ValueBean, Text> {
    private ValueBean outKey = new ValueBean();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ValueBean, Text>.Context context) throws IOException, InterruptedException {
        // 1. 读取每行数据进行处理
        String line = value.toString();

        // 2. 切割出数据且解析需要的数据
        String[] words = line.split("\t");
        String upFlow = words[1];
        String downFlow = words[2];

        // 3. 封装相应的值
        outValue.set(words[0]);
        outKey.setUpFlow(Long.parseLong(upFlow));
        outKey.setDownFlow(Long.parseLong(downFlow));
        outKey.setTotalFlow();



        context.write(outKey, outValue);
    }
}
