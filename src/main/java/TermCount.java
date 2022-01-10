import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class TermCount {

    // 输入<<className,term>,1>，输出<<className,term>,1>
    public static class TermMapper
            extends Mapper<ClassTermPair, IntWritable, ClassTermPair, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private ClassTermPair key = new ClassTermPair();

        public void map(ClassTermPair key, IntWritable value, Context context
        ) throws IOException, InterruptedException {
            this.key.set(key);
            context.write(key, one);
//            System.out.println("map:" + key);
        }
    }

    // 输入<<className,term>,{1,1,...}>，输出<<className,term>,term在class中的出现次数>
    public static class IntSumReducer
            extends Reducer<ClassTermPair,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(ClassTermPair key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            Text resKey = new Text(key.toString());
            context.write(resKey, result);
//            System.out.println("reduce:" + resKey + ',' + result);
        }
    }

    /*
    args[0]:训练集目录路径
    args[1]:结果存放路径
    * */
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "term count");
        job.setJarByClass(TermCount.class);
//        job.setInputFormatClass(TermInputFormat.class);
        job.setInputFormatClass(TermCombineInputFormat.class);
        job.setMapperClass(TermCount.TermMapper.class);
//        job.setCombinerClass(TermCount.IntSumReducer.class);
        job.setReducerClass(TermCount.IntSumReducer.class);
        job.setOutputKeyClass(ClassTermPair.class);
        job.setOutputValueClass(IntWritable.class);

        // set trainset
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        FileStatus[] stats = hdfs.listStatus(path);
        StringBuilder paths = new StringBuilder();
        for (FileStatus stat: stats) {
            paths.append(',').append(stat.getPath());
        }

//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileInputFormat.addInputPaths(job, paths.toString().substring(1));
        FileInputFormat.setMaxInputSplitSize(job, 262144); // 256KB
        FileInputFormat.addInputPaths(job, paths.toString().substring(1));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}