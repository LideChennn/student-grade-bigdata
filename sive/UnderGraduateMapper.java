package cn.edu.gdpu.lastwork.sive;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.lang3.StringUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * 输入
 * 成绩表(id， 学生姓名 ， ...  总分， 省份)
 * 本科线(省份， 本科线)
 *
 *   使用缓存 map join
 *
 * 输出
 * 学号 姓名 总分（满足总分大于本科线） 省份
 */
public class UnderGraduateMapper extends Mapper<LongWritable, Text, GradeBean, NullWritable> {
    private final GradeBean outKey = new GradeBean();
    //key 省份 -- value 本科线
    private final HashMap<String, Integer> underGraduateMap = new HashMap<>();
    @Override
    protected void setup(Mapper<LongWritable, Text, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        //输入流
        FSDataInputStream fis = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader bfr = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNotEmpty(line = bfr.readLine())) {
            String[] split = line.split("\t");
            underGraduateMap.put(split[0], Integer.parseInt(split[1]));
        }
        IOUtils.closeStream(fis);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        int totalScore = Integer.parseInt(split[split.length - 2]);
        String province = split[split.length - 1];

        if (totalScore >= underGraduateMap.get(province)) {//学生总分比对应省的本科线高
            //筛选过本科线的人，进入输出
            outKey.setExamId(split[0]);
            outKey.setName(split[1]);
            outKey.setTotalScore(totalScore);
            outKey.setProvince(province);
            context.write(outKey, NullWritable.get());
        }

    }
}
