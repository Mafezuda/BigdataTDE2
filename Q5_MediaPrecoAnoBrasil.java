public class Q5_MediaPrecoAnoBrasil {
    public static class Map extends Mapper<LongWritable, Text, Text, SomaQtdWritable>{
        @Override
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] f = v.toString().split(";");
            if (f[0].equalsIgnoreCase("Country") || f.length<10) return;
            if (!f[0].equalsIgnoreCase("Brazil")) return;
            try {
                double price = Double.parseDouble(f[5].replace(",", "."));
                c.write(new Text(f[1]), new SomaQtdWritable(price, 1));
            } catch (Exception e) {}
        }
    }
    public static class Combiner extends Reducer<Text, SomaQtdWritable, Text, SomaQtdWritable>{
        @Override
        protected void reduce(Text k, Iterable<SomaQtdWritable> v, Context c) throws IOException, InterruptedException {
            double s=0; long q=0;
            for(SomaQtdWritable w:v){ s+=w.getSoma(); q+=w.getQtd(); }
            c.write(k,new SomaQtdWritable(s,q));
        }
    }
    public static class Reduce extends Reducer<Text, SomaQtdWritable, Text, DoubleWritable>{
        @Override
        protected void reduce(Text k, Iterable<SomaQtdWritable> v, Context c) throws IOException, InterruptedException {
            double s=0; long q=0;
            for(SomaQtdWritable w:v){ s+=w.getSoma(); q+=w.getQtd(); }
            c.write(k,new DoubleWritable(s/q));
        }
    }
}
