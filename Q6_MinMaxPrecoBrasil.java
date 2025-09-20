public class Q6_MinMaxPrecoBrasil2016 {
    public static class Map extends Mapper<LongWritable, Text, Text, TransacaoWritable>{
        private final static Text CHAVE = new Text("BR2016");
        private TransacaoWritable t = new TransacaoWritable();
        @Override
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] f = v.toString().split(";");
            if (f[0].equalsIgnoreCase("Country") || f.length<10) return;
            if (!f[0].equalsIgnoreCase("Brazil") || !"2016".equals(f[1])) return;
            try {
                double price = Double.parseDouble(f[5].replace(",", "."));
                double weight= f[6].isEmpty()?0:Double.parseDouble(f[6].replace(",","."));
                double amount= f[8].isEmpty()?0:Double.parseDouble(f[8].replace(",","."));
                t.set(f[0],2016,f[2],f[3],f[4],price,weight,f[7],amount,f[9]);
                c.write(CHAVE,t);
            }catch(Exception e){}
        }
    }
    public static class Reduce extends Reducer<Text, TransacaoWritable, Text, Text>{
        @Override
        protected void reduce(Text k, Iterable<TransacaoWritable> v, Context c) throws IOException, InterruptedException {
            double min=Double.MAX_VALUE,max=Double.MIN_VALUE;
            TransacaoWritable minT=null,maxT=null;
            for(TransacaoWritable t:v){
                double p = t.getPrice();
                if(p<min){ min=p; minT=t; }
                if(p>max){ max=p; maxT=t; }
            }
            if(minT!=null) c.write(new Text("Mais barata"), new Text(minT.toString()));
            if(maxT!=null) c.write(new Text("Mais cara"),   new Text(maxT.toString()));
        }
    }
}
