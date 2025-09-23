package br.puc.tde.jobs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import br.puc.tde.wtypes.*;

public class Q8_MinMaxAmountAnoPais {
    public static class Map extends Mapper<LongWritable, Text, AnoPaisWritable, TransacaoWritable> {
        private AnoPaisWritable keyOut = new AnoPaisWritable();
        private TransacaoWritable t = new TransacaoWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] f = value.toString().split(";");
            if (f[0].equalsIgnoreCase("country_or_area") || f.length < 10) return;

            try {
                int year = Integer.parseInt(f[1]);
                double amount = f[8].isEmpty() ? 0 : Double.parseDouble(f[8].replace(",", "."));
                double price = f[5].isEmpty() ? 0 : Double.parseDouble(f[5].replace(",", "."));
                double weight = f[6].isEmpty() ? 0 : Double.parseDouble(f[6].replace(",", "."));

                keyOut.set(year, f[0]);
                t.set(f[0], year, f[2], f[3], f[4], price, weight, f[7], amount, f[9]);
                context.write(keyOut, t);
            } catch (Exception e) {}
        }
    }

    public static class Reduce extends Reducer<AnoPaisWritable, TransacaoWritable, Text, Text> {
        private static String formatNumber(double v) {
            if (Double.isFinite(v) && v == Math.rint(v)) return String.format("%d", (long) Math.rint(v));
            return String.format("%.2f", v);
        }

        @Override
        protected void reduce(AnoPaisWritable key, Iterable<TransacaoWritable> values, Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE, max = Double.MIN_VALUE;

            for (TransacaoWritable t : values) {
                double a = t.getAmount();
                if (a < min) min = a;
                if (a > max) max = a;
            }

            context.write(null, new Text(String.format("%s %d Min: %s", key.getPais(), key.getAno(), formatNumber(min))));
            context.write(null, new Text(String.format("%s %d Max: %s", key.getPais(), key.getAno(), formatNumber(max))));
        }
    }
}
