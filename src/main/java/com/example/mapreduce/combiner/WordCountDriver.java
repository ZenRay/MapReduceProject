package com.example.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 job
        // 下面的方式的本地调用，没有使用集群
        Configuration conf = new Configuration();
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

        // 设置 mapper 阶段的 combiner。因为 combiner 处理和 reduce 的处理相同可以直接替换为 reduce 的 class
        job.setCombinerClass(MapCombiner.class);

        // 6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("./data/input/hello.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output/combiner"));

        // 7. 提交 Job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
