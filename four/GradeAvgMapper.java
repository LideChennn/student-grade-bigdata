package cn.edu.gdpu.lastwork.four;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入
 * 1001	张三	90		80		75		90		65		75		   475		广东
 *
 * 输出
 * 输出（分别提取各科的分数字段）
 * 语文 (分数 总数)
 *
 * 数学 (分数 总数)
 *
 * 英语 (分数 总数)
 * ...
 * 分区：根据subject分区，区内按分数降序排序
 */
public class GradeAvgMapper extends Mapper<LongWritable, Text, Text, SumAndCountBean> {
    String[] subjects = {"语文", "数学", "英语", "政治", "生物", "物理"};
    private final Text outKey = new Text();
    private final SumAndCountBean outValue = new SumAndCountBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, SumAndCountBean>.Context context) throws IOException, InterruptedException {
        //10001	茅华	20	22	35	83	86	11	257	江苏
        String[] split = value.toString().split("\t");
        //6个科目
        for (int i = 0; i < 6; i++) {
            outKey.set(subjects[i]);

            outValue.setSum(Integer.parseInt(split[i + 2]));
            outValue.setCount(1);

            context.write(outKey, outValue);
        }
    }
}
