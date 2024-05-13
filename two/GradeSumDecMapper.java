package cn.edu.gdpu.lastwork.two;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 输入
 * 1001	张三	90		80		75		90		65		75		   475		广东
 *
 * 输出
 *  (id name totalScore province) GradeBean按总分降序排序
 *
 *  经过分区（按省份分区）
 *
 */
public class GradeSumDecMapper extends Mapper<LongWritable, Text, GradeBean, NullWritable> {
    private GradeBean outKey = new GradeBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //切割
        String[] split = value.toString().split("\t");

        outKey.setExamId(split[0]);
        outKey.setName(split[1]);
        outKey.setTotalScore(Integer.parseInt(split[split.length - 2]));
        outKey.setProvince(split[split.length - 1]);

        //写出
        context.write(outKey, NullWritable.get());
    }
}
