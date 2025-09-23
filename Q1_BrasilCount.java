package br.puc.tde.jobs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class Q1_BrasilCount {
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] f = value.toString().split(";");
            if (f[0].equalsIgnoreCase("Country") || f.length < 10) return;
            if (f[0].equalsIgnoreCase("Brazil")) {
                context.write(new Text("Brasil"), ONE);
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values) sum += v.get();
            String output = String.format("%-15s %10d", key.toString(), sum);
            context.write(null, new Text(output));
        }
    }
}
