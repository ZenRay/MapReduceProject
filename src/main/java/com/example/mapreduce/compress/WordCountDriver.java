package com.example.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 过程中有三个地方可以选择压缩:
 * 1. Map 输入端
 * 2. Map 输出端
 * 3. Reduce 输出端
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 job
        // 下面的方式的本地调用，没有使用集群
        Configuration conf = new Configuration();

        // 配置 Map 输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);

        // 设置压缩方式
        conf.setClass("mapreduce.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);


        // 2. 获取 Jar 包路径
        job.setJarByClass(WordCountDriver.class);

        // 3. 关联 Mapper 和 Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. 设置 map 输出的 KV 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出的 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/input/words.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output/wordcountcompresss"));

        // 配置 reduce 输出端压缩和压缩方式: 需要启用下面的压缩可以取消下面的注释
//        FileOutputFormat.setCompressOutput(job, true);
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        // 7. 提交 Job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
