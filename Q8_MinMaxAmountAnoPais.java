public class Q8_MinMaxAmountAnoPais {
    public static class Map extends Mapper<LongWritable, Text, AnoPaisWritable, TransacaoWritable>{
        private AnoPaisWritable keyOut = new AnoPaisWritable();
        private TransacaoWritable t = new TransacaoWritable();
        @Override
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] f = v.toString().split(";");
            if (f[0].equalsIgnoreCase("Country") || f.length<10) return;
            try {
                int year = Integer.parseInt(f[1]);
                double amount = Double.parseDouble(f[8].replace(",", "."));
                double price  = Double.parseDouble(f[5].replace(",", "."));
                double weight = f[6].isEmpty()?0:Double.parseDouble(f[6].replace(",", "."));
                keyOut.set(year, f[0]);
                t.set(f[0],year,f[2],f[3],f[4],price,weight,f[7],amount,f[9]);
                c.write(new AnoPaisWritable(year,f[0]), t);
            } catch(Exception e){}
        }
    }
    public static class Reduce extends Reducer<AnoPaisWritable, TransacaoWritable, Text, Text>{
        @Override
        protected void reduce(AnoPaisWritable k, Iterable<TransacaoWritable> v, Context c) throws IOException, InterruptedException {
            double min=Double.MAX_VALUE,max=Double.MIN_VALUE;
            TransacaoWritable minT=null,maxT=null;
            for(TransacaoWritable t:v){
                double a = t.getAmount();
                if(a<min){ min=a; minT=t; }
                if(a>max){ max=a; maxT=t; }
            }
            if(minT!=null) c.write(new Text(k.toString()+" MIN"), new Text(minT.toString()));
            if(maxT!=null) c.write(new Text(k.toString()+" MAX"), new Text(maxT.toString()));
        }
    }
}
