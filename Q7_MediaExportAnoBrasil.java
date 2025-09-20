package br.puc.tde.jobs;

import br.puc.tde.wtypes.SomaQtdWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

/**
 * Q7 – Valor médio das transações por ano,
 * considerando somente as transações do tipo Export
 * realizadas no Brasil.
 */
public class Q7_MediaExportAnoBrasil {

    // ---------- MAPPER ----------
    public static class Map extends Mapper<LongWritable, Text, Text, SomaQtdWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Divide a linha CSV em colunas
            String[] f = value.toString().split(";");
            // Ignora cabeçalho e linhas incompletas
            if (f[0].equalsIgnoreCase("Country") || f.length < 10) return;

            // Filtra: Somente Brasil + Flow Export
            if (!f[0].equalsIgnoreCase("Brazil")) return;
            if (!f[4].equalsIgnoreCase("Export")) return;

            try {
                // Preço (Price) é a coluna 6 (índice 5)
                double price = Double.parseDouble(f[5].replace(",", "."));
                // Ano é a coluna 2 (índice 1)
                String ano = f[1];
                context.write(new Text(ano), new SomaQtdWritable(price, 1));
            } catch (Exception e) {
                // Se houver dados faltantes ou erro de parsing, ignora a linha
            }
        }
    }

    // ---------- COMBINER ----------
    // Combina soma e contagem localmente para reduzir tráfego
    public static class Combiner extends Reducer<Text, SomaQtdWritable, Text, SomaQtdWritable> {
        @Override
        protected void reduce(Text key, Iterable<SomaQtdWritable> values, Context context)
                throws IOException, InterruptedException {
            double soma = 0;
            long qtd = 0;
            for (SomaQtdWritable v : values) {
                soma += v.getSoma();
                qtd  += v.getQtd();
            }
            context.write(key, new SomaQtdWritable(soma, qtd));
        }
    }

    // ---------- REDUCER ----------
    public static class Reduce extends Reducer<Text, SomaQtdWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<SomaQtdWritable> values, Context context)
                throws IOException, InterruptedException {
            double soma = 0;
            long qtd = 0;
            for (SomaQtdWritable v : values) {
                soma += v.getSoma();
                qtd  += v.getQtd();
            }
            if (qtd > 0) {
                context.write(key, new DoubleWritable(soma / qtd));
            }
        }
    }
}
