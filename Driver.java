package br.puc.tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import br.puc.tde.wtypes.*;
import br.puc.tde.jobs.*;

public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: Driver <q1..q8> <input> <output>");
            System.exit(1);
        }
        String questao = args[0];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, questao);

        job.setJarByClass(Driver.class);

        switch (questao) {
            case "q1":
                job.setMapperClass(Q1_BrasilCount.Map.class);
                job.setReducerClass(Q1_BrasilCount.Reduce.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                break;
            case "q2":
                job.setMapperClass(Q2_PorAno.Map.class);
                job.setReducerClass(Q2_PorAno.Reduce.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                break;
            // Repita o padrão para q3 .. q8
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
