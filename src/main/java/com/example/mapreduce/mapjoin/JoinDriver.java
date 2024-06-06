package com.example.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 当过多的合并流程放在 Reduce 端处理时，非常容易产生数据倾斜。如果某些处理可以放在 Map 端来处理的话
 * 可以降低 Reduce 短短数据压力，减少数据倾斜。例如需要进行合并的一些数据处理，可以将小文件以 cache 的
 * 方式存储，在 map 端进行合并
 */
public class JoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        // 1.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.
        job.setJarByClass(JoinDriver.class);

        // 3.
        job.setMapperClass(JoinMapper.class);

        // 4.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        File cacheFile = new File("./data/input/join/product.txt");

        job.addCacheFile(new URI("file://" + cacheFile.getAbsoluteFile()));
        // Map 端已经完成了 JOIN ，因此不再需要 Reduce 阶段，将 Reduce 的任务数设置为 0
        job.setNumReduceTasks(0);

        // 6.
        FileInputFormat.setInputPaths(job, new Path("./data/input/join/orders.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output/mapjoin "));

        // 7.
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1 );

    }
}
