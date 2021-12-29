import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Prediction {
    private static HashMap<String, Double> classProbsLog;   // 先验概率取对数
    private static HashMap<ClassTermPair, Integer> termNumbers; // 各类中各单词的数量
    private static HashMap<String, Integer> classTermCounts;    // 各类单词总数

    public static <K> void add(HashMap<K, Integer> hm, K key, int n) {
        if (hm.containsKey(key)) {
            hm.put(key, hm.get(key) + n);
        } else {
            hm.put(key, n);
        }
    }

    // 从文件读取先验概率
    public static void readClassProbs(FileSystem fs, Path path) throws IOException {
        classProbsLog = new HashMap<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        String[] cn;
        ArrayList<String> classNames = new ArrayList<>();
        ArrayList<Integer> docNumbers = new ArrayList<>();

        while ((line = br.readLine()) != null) {
            cn = line.split("\\s+");
            classNames.add(cn[0]);
            docNumbers.add(Integer.parseInt(cn[1]));
        }

        int sum = 0;
        for (int docNumber: docNumbers) {
            sum += docNumber;
        }

        for (int i = 0; i < classNames.size(); i++) {
            classProbsLog.put(
                    classNames.get(i),
                    Math.log10((double) docNumbers.get(i)) - Math.log10(sum));
        }

        br.close();
    }

    // 从文件读取各类中各单词数量及单词总数
    public static void readTermProbs(FileSystem fs, Path path) throws IOException {
        termNumbers = new HashMap<>();
        classTermCounts = new HashMap<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        String[] cn;
        int classTermNumber;

        while ((line = br.readLine()) != null) {
            cn = line.split("\\s+");
            classTermNumber = Integer.parseInt(cn[2]) + 1;  // 防止不存在的term使条件概率为0
            add(classTermCounts, cn[0], classTermNumber);

            termNumbers.put(
                    new ClassTermPair(cn[0], cn[1]),
                    classTermNumber)
            ;
        }

        br.close();
    }

    // 将doc内容分割成单词
    public static String[] getTerms(String content) {
        return content.split("\n");
    }

    // 计算文档(内容为content)属于某一类(类名className)的条件概率
    public static double conditionProbabilityForClass(String content, String className) {
        double classProbLog = classProbsLog.get(className);
        double conditionProbLog = 0.;
        double classTermCountLog = Math.log10((double) classTermCounts.get(className));
        String[] terms = getTerms(content);

        ClassTermPair ctp = new ClassTermPair();
        ctp.setClassName(className);

        for (String term: terms) {
            ctp.setTerm(term);
            conditionProbLog -= classTermCountLog;
            if (termNumbers.containsKey(ctp)) {
                conditionProbLog += Math.log10((double) termNumbers.get(ctp));
            }
        }

        return classProbLog + conditionProbLog;
    }

    public static class DocMapper
            extends Mapper<Text, Text, Text, ClassProbPair> {

        private Text key = new Text();
        private ClassProbPair value = new ClassProbPair();

        public void map(Text docId, Text content, Context context
        ) throws IOException, InterruptedException {
            this.key.set(docId);

            double conditionProbabilityLog;
            for (String className: classProbsLog.keySet()) {
                conditionProbabilityLog = conditionProbabilityForClass(content.toString(), className);
                this.value.setClassName(className);
                this.value.setProbLog(conditionProbabilityLog);

                context.write(this.key, this.value);
            }
//            System.out.println("map:" + key);
        }
    }

    public static class ProbMaxReducer
            extends Reducer<Text, ClassProbPair, Text, Text> {

        public void reduce(Text docId, Iterable<ClassProbPair> probLogs,
                           Context context
        ) throws IOException, InterruptedException {
            double maxProbLog = -Double.MAX_VALUE;
            String maxProbClassName = "";
            for (ClassProbPair cpp: probLogs
                 ) {
                if (cpp.getProbLog() > maxProbLog) {
                    maxProbLog = cpp.getProbLog();
                    maxProbClassName = cpp.getClassName();
//                    System.out.print(maxProbClassName + "," + maxProbLog + ",");
                }
            }
//            System.out.println();

            context.write(docId, new Text(maxProbClassName));
            System.out.println("reduce:" + docId + ',' + maxProbClassName);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "class prediction");
        job.setJarByClass(Prediction.class);
//        job.setInputFormatClass(DocInputFormat.class);
        job.setInputFormatClass(DocCombineInputFormat.class);
        job.setMapperClass(Prediction.DocMapper.class);
        job.setReducerClass(Prediction.ProbMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ClassProbPair.class);

        // set testset
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        FileStatus[] stats = hdfs.listStatus(path);
        StringBuilder paths = new StringBuilder();
        for (FileStatus stat: stats) {
            paths.append(',').append(stat.getPath());
        }

        FileInputFormat.setMaxInputSplitSize(job, 3145728); // 3MB
        FileInputFormat.addInputPaths(job, paths.toString().substring(1));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        readClassProbs(hdfs, new Path(args[2]));
        readTermProbs(hdfs, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}