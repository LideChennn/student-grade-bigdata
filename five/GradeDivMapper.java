package cn.edu.gdpu.lastwork.five;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输出
 * 省份 分数
 */
public class GradeDivMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable();


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //数据格式：考号 姓名 语文 数学 英语 政治 生物 物理 总分 省份
        String[] split = value.toString().split("\t");

        outKey.set(split[split.length - 1]);//省份
        outValue.set(Integer.parseInt(split[split.length - 2]));//总分

        context.write(outKey, outValue);
    }
}
