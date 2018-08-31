package com.avro.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AvroFileInputDriver extends Configured implements Tool {
    public static class RowMapper extends Mapper<AvroKey<GenericData.Record>, NullWritable, Text, Text> {
        @Override
        public void map(AvroKey<GenericData.Record> key, NullWritable value, Context context) throws IOException, InterruptedException {
            CharSequence id = (CharSequence) key.datum().get(context.getConfiguration().get("keyField"));
            GenericRecord genericRecord = key.datum();
            context.write(new Text(id.toString()), new Text(genericRecord.toString()));
        }
    }

    public static class RowReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, List<String>> datas = new HashMap<>();
            for (Text val : values) {
                Map<String, String> map = mapper.readValue(val.toString(), Map.class);
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    List<String> list = datas.get(entry.getKey());
                    if (list == null) {
                        list = new ArrayList<>();
                        if (!isStringEmptyOrNull(entry.getValue())) {
                            list.add(entry.getValue());
                        } else {
                            list.add("");
                        }
                        datas.put(entry.getKey(), list);
                    } else {
                        if (!list.contains(entry.getValue()) && !isStringEmptyOrNull(entry.getValue())) {
                            if (list.size() == 1 && isStringEmptyOrNull(list.get(0))) {
                                list.clear();
                            }
                            list.add(entry.getValue());
                        }
                    }
                }
            }
            Collection<List<String>> lists = datas.values();
            Set<String> columns = datas.keySet();
            Collection collection = shuffleData(columns, key.toString(), context.getConfiguration().get("outDir"), lists.toArray(new List[lists.size()]));
            context.write(key, new IntWritable(collection.size()));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("keyField", "EMPLOYEE_ID");
        conf.set("outDir", args[1]);

        Job job = new Job(conf);
        job.setJarByClass(AvroFileInputDriver.class);
        job.setJobName("Hive data splitter");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(RowMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setReducerClass(RowReducer.class);
        job.setNumReduceTasks(1);

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.submit();

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static Collection<List<String>> shuffleData(Set<String> columns, String id, String outFilePath, List<String>... list) throws IOException {
        System.out.println("Splitting row for id... " + id);
        Stream<Collection<String>> inputs = Stream.of(list);
        Stream<Collection<List<String>>> listified = inputs.filter(Objects::nonNull)
                .filter(input -> !input.isEmpty())
                .map(l -> l.stream()
                        .map(o -> new ArrayList<>(Arrays.asList(o)))
                        .collect(Collectors.toList()));

        Collection<List<String>> combinations = listified.reduce((input1, input2) -> {
            Collection<List<String>> merged = new ArrayList<>();
            input1.forEach(permutation1 -> input2.forEach(permutation2 -> {
                List<String> combination = new ArrayList<>();
                combination.addAll(permutation1);
                combination.addAll(permutation2);
                merged.add(combination);
            }));
            return merged;
        }).orElse(new HashSet<>());

        System.out.println("Shuffle completed");
        writeDataIntoAvroFile(combinations, columns, outFilePath);
        return combinations;
    }

    private static void writeDataIntoAvroFile(Collection<List<String>> combinations, Set<String> columns, String outFilePath) throws IOException {
        Schema avroSchema = createAvroSchema(new ArrayList<>(columns));
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            File avroFile = new File(outFilePath + "/result.avro");
            if (avroFile.exists()) {
                dataFileWriter.appendTo(avroFile);
            } else {
                dataFileWriter.create(avroSchema, avroFile);
            }
            GenericRecord genericRecord = new GenericData.Record(avroSchema);

            for (List<String> combination : combinations) {
                int i = 0;
                for (String column : columns) {
                    genericRecord.put(column, combination.get(i++));
                }
                dataFileWriter.append(genericRecord);
            }
        } finally {
            dataFileWriter.close();
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvroFileInputDriver(), args);
        System.exit(res);
    }

    private static Schema createAvroSchema(List<String> fieldNames) {
        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record("rt").fields();

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            assembler = assembler.optionalString(fieldName);
        }
        Schema schema = assembler.endRecord();
        return schema;
    }

    private static boolean isStringEmptyOrNull(String string) {
        return string == null || string.trim().length() == 0;
    }

}