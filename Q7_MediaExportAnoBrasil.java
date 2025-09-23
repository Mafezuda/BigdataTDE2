package br.puc.tde.jobs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import br.puc.tde.wtypes.SomaQtdWritable;

public class Q7_MediaExportAnoBrasil {
    public static class Map extends Mapper<LongWritable, Text, Text, SomaQtdWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] f = value.toString().split(";");
            if(f[0].equalsIgnoreCase("Country") || f.length < 10) return;
            if(!f[0].equalsIgnoreCase("Brazil") || !"Export".equalsIgnoreCase(f[4])) return;
            try {
                double price = Double.parseDouble(f[5].replace(",", "."));
                context.write(new Text(f[1]), new SomaQtdWritable(price,1));
            } catch(Exception e){}
        }
    }

    public static class Combiner extends Reducer<Text, SomaQtdWritable, Text, SomaQtdWritable> {
        @Override
        protected void reduce(Text key, Iterable<SomaQtdWritable> values, Context context) throws IOException, InterruptedException {
            double s=0; long q=0;
            for(SomaQtdWritable v: values){ s+=v.getSoma(); q+=v.getQtd(); }
            context.write(key,new SomaQtdWritable(s,q));
        }
    }

    public static class Reduce extends Reducer<Text, SomaQtdWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<SomaQtdWritable> values, Context context) throws IOException, InterruptedException {
            double s=0; long q=0;
            for(SomaQtdWritable v: values){ s+=v.getSoma(); q+=v.getQtd(); }
            if(q>0) context.write(key,new DoubleWritable(s/q));
        }
    }
}
