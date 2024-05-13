package cn.edu.gdpu.lastwork.three;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入
 * 1001	张三	90		80		75		90		65		75		   475		广东
 * 输出（分别提取各科的分数字段）
 * 语文 分数 id name
 *
 * 数学 分数 id name
 *
 * 英语 分数 id name
 * ...
 * 分区：根据subject分区，区内按分数降序排序，分数一样则按照
 */
public class GradeTop100Mapper extends Mapper<LongWritable, Text, GradeBean, NullWritable> {
    private final GradeBean outKey = new GradeBean();
    private final String[] subjects = {"语文", "数学", "英语", "政治", "生物", "物理"};
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //00001	计明	72	67	96	13	91	61	400	广东
        //00002	秦俊	17	47	96	88	96	14	358	广东
        String[] split = value.toString().split("\t");

        for (int i = 0; i < 6; i++) {
            //输出    科目  分数 id name
            outKey.setSubject(subjects[i]);//根据科目分区

            outKey.setExamId(split[0]);
            outKey.setScore(Integer.parseInt(split[i + 2]));
            outKey.setName(split[1]);
            context.write(outKey, NullWritable.get());
        }
    }
}
