package com.example.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, KeyBean, KeyBean> {
    private KeyBean outKey = new KeyBean();
    private KeyBean outValue = new KeyBean();

    @Override
    protected void map(
            LongWritable key, Text value, Mapper<LongWritable, Text, KeyBean, KeyBean>.Context context
    ) throws IOException, InterruptedException {
        // 1. 读取每行数据进行处理
        String line = value.toString();

        // 2. 切割出数据且解析需要的数据
        String[] words = line.split("\t");
        String upFlow = words[words.length - 3];
        String downFlow = words[words.length - 2];

        // 3. 封装相应的值
        outKey.setPhone(words[1]);
        outValue.setUpFlow(Long.parseLong(upFlow));
        outValue.setDownFlow(Long.parseLong(downFlow));
        outValue.setTotalFlow();

        context.write(outKey, outValue);
    }
}
