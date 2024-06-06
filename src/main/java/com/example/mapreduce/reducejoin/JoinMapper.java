package com.example.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    // 根据不同的切片文件名称进行不同的处理
    private String fileName;
    private Text outputKey = new Text();
    private TableBean outputValue = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");


        // 根据不同的文件进行不同的处理
        if (fileName.contains("order")){
            // 将产品ID 作为 key
            outputKey.set(words[1]);

            outputValue.setOrderId(words[0]);
            outputValue.setProductId(words[1]);
            outputValue.setAmount(Integer.parseInt(words[2]));
            outputValue.setProductName("");
            outputValue.setTableFlag("order");


        } else if (fileName.contains("product")) {
            outputKey.set(words[0]);
            outputValue.setOrderId("");
            outputValue.setProductId(words[0]);
            outputValue.setAmount(0);
            outputValue.setProductName(words[1]);
            outputValue.setTableFlag("product");

        }
        context.write(outputKey, outputValue);
    }
}
