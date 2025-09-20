package br.puc.tde.wtypes;

import org.apache.hadoop.io.Writable;
import java.io.*;

public class TransacaoWritable implements Writable {
    private String country, code, commodity, flow, unit, category;
    private int year;
    private double price, weight, amount;

    public TransacaoWritable() {}

    public void set(String c,int y,String cd,String cm,String fl,
                    double p,double w,String u,double a,String cat){
        country=c;year=y;code=cd;commodity=cm;flow=fl;
        price=p;weight=w;unit=u;amount=a;category=cat;
    }

    public double getPrice(){ return price; }
    public double getAmount(){ return amount; }
    public int getYear(){ return year; }
    public String getCountry(){ return country; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(country); out.writeInt(year); out.writeUTF(code);
        out.writeUTF(commodity); out.writeUTF(flow); out.writeDouble(price);
        out.writeDouble(weight); out.writeUTF(unit); out.writeDouble(amount);
        out.writeUTF(category);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country=in.readUTF(); year=in.readInt(); code=in.readUTF();
        commodity=in.readUTF(); flow=in.readUTF(); price=in.readDouble();
        weight=in.readDouble(); unit=in.readUTF(); amount=in.readDouble();
        category=in.readUTF();
    }

    @Override
    public String toString() {
        return country+";"+year+";"+code+";"+commodity+";"+flow+";"+
               price+";"+weight+";"+unit+";"+amount+";"+category;
    }
}
