package br.puc.tde.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import br.puc.tde.jobs.*;
import br.puc.tde.wtypes.SomaQtdWritable;

public class Driver {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        // Diretório de entrada
        Path inputDir = new Path("input/transactions"); // coloque seu diretório de CSVs
        // Diretórios de saída
        Path outQ1 = new Path("output/Q1_BrasilCount");
        Path outQ2 = new Path("output/Q2_PorAno");
        Path outQ3 = new Path("output/Q3_PorCategoria");
        Path outQ4 = new Path("output/Q4_PorFluxo");
        Path outQ5 = new Path("output/Q5_MediaPrecoAnoBrasil");
        Path outQ6 = new Path("output/Q6_MinMaxPrecoBrasil2016");
        Path outQ7 = new Path("output/Q7_MediaExportAnoBrasil");
        Path outQ8 = new Path("output/Q8_MinMaxAmountAnoPais");

        // --- Q1 ---
        Job job1 = Job.getInstance(conf, "Q1_BrasilCount");
        job1.setJarByClass(Q1_BrasilCount.class);
        job1.setMapperClass(Q1_BrasilCount.Map.class);
        job1.setReducerClass(Q1_BrasilCount.Reduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, inputDir);
        FileOutputFormat.setOutputPath(job1, outQ1);
        if(!job1.waitForCompletion(true)) return;

        // --- Q2 ---
        Job job2 = Job.getInstance(conf, "Q2_PorAno");
        job2.setJarByClass(Q2_PorAno.class);
        job2.setMapperClass(Q2_PorAno.Map.class);
        job2.setReducerClass(Q2_PorAno.Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, inputDir);
        FileOutputFormat.setOutputPath(job2, outQ2);
        if(!job2.waitForCompletion(true)) return;

        // --- Q3 ---
        Job job3 = Job.getInstance(conf, "Q3_PorCategoria");
        job3.setJarByClass(Q3_PorCategoria.class);
        job3.setMapperClass(Q3_PorCategoria.Map.class);
        job3.setReducerClass(Q3_PorCategoria.Reduce.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job3, inputDir);
        FileOutputFormat.setOutputPath(job3, outQ3);
        if(!job3.waitForCompletion(true)) return;

        // --- Q4 ---
        Job job4 = Job.getInstance(conf, "Q4_PorFluxo");
        job4.setJarByClass(Q4_PorFluxo.class);
        job4.setMapperClass(Q4_PorFluxo.Map.class);
        job4.setReducerClass(Q4_PorFluxo.Reduce.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job4, inputDir);
        FileOutputFormat.setOutputPath(job4, outQ4);
        if(!job4.waitForCompletion(true)) return;

        // --- Q5 ---
        Job job5 = Job.getInstance(conf, "Q5_MediaPrecoAnoBrasil");
        job5.setJarByClass(Q5_MediaPrecoAnoBrasil.class);
        job5.setMapperClass(Q5_MediaPrecoAnoBrasil.Map.class);
        job5.setCombinerClass(Q5_MediaPrecoAnoBrasil.Combiner.class);
        job5.setReducerClass(Q5_MediaPrecoAnoBrasil.Reduce.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(SomaQtdWritable.class);
        FileInputFormat.addInputPath(job5, inputDir);
        FileOutputFormat.setOutputPath(job5, outQ5);
        if(!job5.waitForCompletion(true)) return;

        // --- Q6 ---
        Job job6 = Job.getInstance(conf, "Q6_MinMaxPrecoBrasil2016");
        job6.setJarByClass(Q6_MinMaxPrecoBrasil2016.class);
        job6.setMapperClass(Q6_MinMaxPrecoBrasil2016.Map.class);
        job6.setReducerClass(Q6_MinMaxPrecoBrasil2016.Reduce.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(br.puc.tde.wtypes.TransacaoWritable.class);
        FileInputFormat.addInputPath(job6, inputDir);
        FileOutputFormat.setOutputPath(job6, outQ6);
        if(!job6.waitForCompletion(true)) return;

        // --- Q7 ---
        Job job7 = Job.getInstance(conf, "Q7_MediaExportAnoBrasil");
        job7.setJarByClass(Q7_MediaExportAnoBrasil.class);
        job7.setMapperClass(Q7_MediaExportAnoBrasil.Map.class);
        job7.setCombinerClass(Q7_MediaExportAnoBrasil.Combiner.class);
        job7.setReducerClass(Q7_MediaExportAnoBrasil.Reduce.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(SomaQtdWritable.class);
        FileInputFormat.addInputPath(job7, inputDir);
        FileOutputFormat.setOutputPath(job7, outQ7);
        if(!job7.waitForCompletion(true)) return;

        // --- Q8 ---
        Job job8 = Job.getInstance(conf, "Q8_MinMaxAmountAnoPais");
        job8.setJarByClass(Q8_MinMaxAmountAnoPais.class);
        job8.setMapperClass(Q8_MinMaxAmountAnoPais.Map.class);
        job8.setReducerClass(Q8_MinMaxAmountAnoPais.Reduce.class);
        job8.setOutputKeyClass(br.puc.tde.wtypes.AnoPaisWritable.class);
        job8.setOutputValueClass(br.puc.tde.wtypes.TransacaoWritable.class);
        FileInputFormat.addInputPath(job8, inputDir);
        FileOutputFormat.setOutputPath(job8, outQ8);
        if(!job8.waitForCompletion(true)) return;

        System.out.println("Todos os jobs executados com sucesso!");
    }
}
