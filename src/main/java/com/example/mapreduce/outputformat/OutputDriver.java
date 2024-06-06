package com.example.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.
        job.setJarByClass(OutputDriver.class);

        // 3.
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReducer.class);

        // 4.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义的 outputformat
        job.setOutputFormatClass(CustomizeOutputFormat.class);

        // 6.
        FileInputFormat.setInputPaths(job, new Path("./data/input/outputformat.txt"));

        // 自定义的 outputformat 用于存储最终的目标数据，但是最终输出需要一个目录用于存储 _SUCCESS 文件，因此还是需要有一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("./data/output/outputformat"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0: 1);


    }
}
