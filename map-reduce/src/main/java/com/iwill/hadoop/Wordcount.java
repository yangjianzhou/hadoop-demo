package com.iwill.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Wordcount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        /**
         * 这个mapper类是一个泛型,它有四个形参类型，分别指定map函数的输入key，输入value，输出key、输出value的类型
         * hadoop没有直接使用java内嵌的类型，而是自己开发了一套可以优化网络序列化传输的基本类型，这些类型在org.apache.hadoop.io包中
         * 这个例子中的Object类型，适用于字段需要使用多种类型的情况，Text类型相当于java中的String类型
         * IntWritable相当于java中的Integer类型
         *
         * Context是mapper的一个内部类，用于在map或者reduce任务中跟踪task的状态，mapContext记录了map执行的上下文，
         * 在mapper类中，context可以存储一些job conf的信息，比如job运行时的参数等
         * 可以在map函数中处理这些信息。同时context也充当了map和reduce任务执行过程中各个函数之间的桥梁
         * context对象保存了作业运行的上下文信息，比如作业配置信息、inputSplit信息、任务IDdeng
         *
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件
         *
         */
        Configuration conf = new Configuration();
        /**
         * job对象指定了作业执行规范，可以用它来控制整个作业的运行
         */
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Wordcount.class);
        /**
         * 在hadoop集群上运行作业时，要把代码打包成一个jar文件，然后把此文件上传到集群上，并通过命令类执行这个作业
         * 但是命令中不必指定jar文件的名称。在这个命令中，通过job对象的setJarByClass方法传递一个主类即可
         * hadoop会通过这个主类来查找包含它的jar文件
         */
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /**
         * 一般情况下，mapper和reducer输出的数据类型是一样的，所以可以用上面两条命令
         * 如果不一样，可以用下面两条命令单独指定mapper输出的key、value的数据类型
         *
         */
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        /**
         * input path 指定的路径可以是单个文件，一个目录或符合特定文件模式的一系列文件
         */
        FileInputFormat.addInputPath(job, new Path("/Users/jiyang12/Github/hadoop-demo/map-reduce/src/main/resources/input/1.txt"));
        /**
         * 只能是一个输出路径，该路径指定的是reduce函数输出文件的写入目录
         * 特别注意：输出目录不能提前存在，否则hadoop会报错并拒绝执行作业，这样做的目的是防止数据丢失
         */
        FileOutputFormat.setOutputPath(job, new Path("/Users/jiyang12/Github/hadoop-demo/map-reduce/src/main/resources/output"));
        /**
         * job.waitForCompletion 提交作业，并等待执行完成，该方法返回一个布尔值，表示执行成功或失败，该布尔值被转换成程序退出代码0或者1
         * 该布尔值还是一个细节标识，所以作业会把进度写入控制台
         * 提交作业后，每秒会轮训作业的进度，如果发现和上次报告后有改变
         * 就会把进度报告到控制台，作业完成后，如果成功就显示作业计数器
         * 如果失败，就把作业失败的原因输出到控制台
         */
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
