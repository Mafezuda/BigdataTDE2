package br.puc.tde.jobs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class Q1_BrasilCount {
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>{
        private static final LongWritable ONE = new LongWritable(1);
        @Override
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] f = v.toString().split(";");
            if (f[0].equalsIgnoreCase("Country") || f.length<10) return;
            if (f[0].equalsIgnoreCase("Brazil"))
                c.write(new Text("Brasil"), ONE);
        }
    }
    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text k, Iterable<LongWritable> vals, Context c) throws IOException, InterruptedException {
            long soma=0; for(LongWritable v:vals) soma+=v.get();
            c.write(k,new LongWritable(soma));
        }
    }
}
