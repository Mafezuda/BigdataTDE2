package br.puc.tde.wtypes;

import org.apache.hadoop.io.*;
import java.io.*;

public class AnoPaisWritable implements WritableComparable<AnoPaisWritable> {
    private IntWritable ano = new IntWritable();
    private Text pais = new Text();

    public AnoPaisWritable() {}
    public AnoPaisWritable(int ano, String pais) {
        this.ano.set(ano);
        this.pais.set(pais);
    }

    public void set(int ano, String pais) {
        this.ano.set(ano);
        this.pais.set(pais);
    }

    public int getAno() { return ano.get(); }
    public String getPais() { return pais.toString(); }

    @Override
    public void write(DataOutput out) throws IOException {
        ano.write(out);
        pais.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ano.readFields(in);
        pais.readFields(in);
    }

    @Override
    public int compareTo(AnoPaisWritable o) {
        int cmp = ano.compareTo(o.ano);
        return cmp != 0 ? cmp : pais.compareTo(o.pais);
    }

    @Override
    public String toString() {
        return ano + "\t" + pais;
    }
}
