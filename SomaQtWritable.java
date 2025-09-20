package br.puc.tde.wtypes;

import org.apache.hadoop.io.Writable;
import java.io.*;

public class SomaQtdWritable implements Writable {
    private double soma;
    private long qtd;

    public SomaQtdWritable() {}
    public SomaQtdWritable(double soma, long qtd) {
        this.soma = soma;
        this.qtd = qtd;
    }

    public double getSoma() { return soma; }
    public long getQtd() { return qtd; }

    public void add(SomaQtdWritable o){
        this.soma += o.soma;
        this.qtd  += o.qtd;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeLong(qtd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        qtd  = in.readLong();
    }
}
