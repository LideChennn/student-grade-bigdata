package cn.edu.gdpu.lastwork.generatedata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class TestDataGenerator {
    private static final String[] PROVINCES = {"广东", "江苏", "浙江"};
    private static final int NUM_STUDENTS = 100;
    private static final String[] FAMILY_NAMES = {
            "赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈", "褚", "卫", "蒋", "沈", "韩", "杨",
            "朱", "秦", "尤", "许", "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏", "陶", "姜",
            "戚", "谢", "邹", "喻", "柏", "水", "窦", "章", "云", "苏", "潘", "葛", "奚", "范", "彭", "郎",
            "鲁", "韦", "昌", "马", "苗", "凤", "花", "方", "俞", "任", "袁", "柳", "酆", "鲍", "史", "唐",
            "费", "廉", "岑", "薛", "雷", "贺", "倪", "汤", "滕", "殷", "罗", "毕", "郝", "邬", "安", "常",
            "乐", "于", "时", "傅", "皮", "卞", "齐", "康", "伍", "余", "元", "卜", "顾", "孟", "平", "黄",
            "和", "穆", "萧", "尹", "姚", "邵", "湛", "汪", "祁", "毛", "禹", "狄", "米", "贝", "明", "臧",
            "计", "伏", "成", "戴", "谈", "宋", "茅", "庞", "熊", "纪", "舒", "屈", "项", "祝", "董", "梁"
    };
    private static final String[] GIVEN_NAMES = {
            "伟", "芳", "娜", "敏", "静", "秀英", "丽", "强", "磊", "洋", "艳", "勇", "军", "杰", "娟", "涛",
            "超", "明", "霞", "秀兰", "刚", "平", "燕", "辉", "玲", "桂英", "丹", "萍", "鹏", "华", "红", "玉",
            "兰", "飞", "霞", "俊", "英", "楠", "梅", "鑫", "波", "斌", "蓉", "宇", "浩", "凯", "秋", "珍",
            "白", "佳", "峰", "翠", "晶", "志", "天", "雪", "海", "婷", "瑞", "文", "宁", "雅", "芬", "龙",
            "琳", "慧", "旭", "宏", "晨", "纯", "如", "阳", "青", "翔", "亮", "建", "文", "瑶", "兴", "宜",
            "婧", "婕", "瑜", "博", "良", "璐", "茹", "菁", "雯", "欣", "晋", "薇", "菲", "伦", "彬", "果",
            "春", "杨", "瑾", "锋", "颖", "荣", "璇", "淑", "晟", "欢", "耀", "瑞", "丽", "洁", "佩", "嘉"};

    public static void main(String[] args) {
        Random random = new Random();
        try {
            for (int j = 0; j < PROVINCES.length; j++) {
                String province = PROVINCES[j];
                BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\Users\\86182\\Desktop\\" + province + "_students.txt"));

                for (int i = 0; i < NUM_STUDENTS; i++) {
                    String examId = String.format("%s%04d", j, i + 1);
                    String name = generateRandomName(random);
                    int chinese = generateScore(random);
                    int math = generateScore(random);
                    int english = generateScore(random);
                    int politics = generateScore(random);
                    int biology = generateScore(random);
                    int physics = generateScore(random);
                    int totalScore = chinese + math + english + politics + biology + physics;

                    String line = String.join("\t", examId, name, String.valueOf(chinese), String.valueOf(math),
                            String.valueOf(english), String.valueOf(politics), String.valueOf(biology),
                            String.valueOf(physics), String.valueOf(totalScore), province);
                    writer.write(line);
                    writer.newLine();
                }
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static int generateScore(Random random) {
        int score;
        if (random.nextDouble() < 0.75) {
            // 75%概率生成大于60分的分数
            score = 60 + random.nextInt(41);
        } else {
            // 25%概率生成小于等于60分的分数
            score = random.nextInt(61);
        }
        return score;
    }
    private static String generateRandomName(Random random) {
        String familyName = FAMILY_NAMES[random.nextInt(FAMILY_NAMES.length)];
        String givenName = GIVEN_NAMES[random.nextInt(GIVEN_NAMES.length)];
        return familyName + givenName;
    }
}