package com.example.mapreduce.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private HashMap<String, String> product = new HashMap<>();
    private Text outputKey = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取缓存文件，将文件缓存到集合中
        URI[] cacheFiles = context.getCacheFiles();

        // 需要将缓存的文件数据，读取出来
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        BufferedReader reader = null;
        try{
            FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
            reader = new BufferedReader(new InputStreamReader(inputStream, "utf8"));

            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())){
                String[] words = line.split("\t");
                product.put(words[0], words[1]);

            }

        }finally {
            // 关闭数据流
            IOUtils.closeStream(reader);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String string = value.toString();

        String[] words = string.split("\t");

        // 封装数据
        outputKey.set(words[0] + "\t" + product.get(words[1]) + "\t" + words[2]);
        context.write(outputKey, NullWritable.get());
    }
}
