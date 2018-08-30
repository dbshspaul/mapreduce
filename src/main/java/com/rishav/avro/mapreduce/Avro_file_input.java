package com.rishav.avro.mapreduce;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class Avro_file_input extends Configured implements Tool {
    public static class ColorCountMapper extends Mapper<AvroKey<GenericData.Record>, NullWritable, Text , IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(AvroKey<GenericData.Record> key, NullWritable value, Context context) throws IOException, InterruptedException {
            CharSequence country =   (CharSequence) key.datum().get("username") ;
            context.write(new Text(country.toString()), one);
        }
    }
    public static class ColorCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public int run(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.err.println("Usage: MapReduceColorCount <input path> <output path>");
//            return -1;
//        }
        Configuration conf = new Configuration();
        Job job = new Job();
        job.setJarByClass(Avro_file_input.class);
        job.setJobName("Color Count");
        FileInputFormat.setInputPaths(job, new Path("/home/debashis/avro-cli-examples-master/twitter.snappy.avro"));
        FileOutputFormat.setOutputPath(job, new Path("/home/debashis/avro-cli-examples-master/out"));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(ColorCountMapper.class);

        Schema schema = new Schema.Parser().parse(new File("/home/debashis/avro-cli-examples-master/twitter.avsc"));
        AvroJob.setInputKeySchema(job, schema);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(ColorCountReducer.class);
        job.setNumReduceTasks(1);

        Path outputPath = new Path("/home/debashis/avro-cli-examples-master/out");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Avro_file_input(), args);
        System.exit(res);
    }
}