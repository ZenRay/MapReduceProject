package com.example.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCountMapper extends WordCountMapper<LongWritable, Text, Text, IntWritable> {
}
