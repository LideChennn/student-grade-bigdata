package cn.edu.gdpu.lastwork.five2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入：
 * 第二题的输出
 *
 * 输出
 * 广东  455
 * 广东  411
 * ...
 *
 * 江苏 582
 * 江苏 412
 * ...
 *
 * 浙江 562
 * 浙江 452
 * ...
 */
public class GradeLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //00027	周菲	440	广东
        String[] split = value.toString().split("\t");

        outKey.set(split[split.length - 1]);//省份
        outValue.set(Integer.parseInt(split[split.length - 2]));//分数

        context.write(outKey, outValue);
    }
}
