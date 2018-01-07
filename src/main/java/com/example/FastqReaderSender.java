package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.github.lindenb.jbwa.jni.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by plaban on 23/6/17.
 */
public class FastqReaderSender extends BaseOperator implements InputOperator
{
    //KSeq kSeq;
    String fastqPath;

    public final transient DefaultOutputPort out = new DefaultOutputPort<String>();


   /* FastqReaderSender()
    {


        try
        {
            kSeq = new KSeq(new File(getFastqPath()));

        }

        catch (IOException e)
        {
            e.printStackTrace();
        }

    }*/

    void setFastqPath(String fastqPath)
    {
        this.fastqPath = fastqPath;
    }

    String getFastqPath()
    {
        return this.fastqPath;
    }

    @Override
    public void emitTuples()
    {
        /*System.load("/home/plaban/Downloads/jbwa-master/src/main/native/libbwajni.so");

        /*try
        {
            BwaIndex index=new BwaIndex(new File("/home/plaban/Downloads/jbwa-master/test/ref.fa"));
            BwaMem mem=new BwaMem(index);
            KSeq kseq=new KSeq(new File("/home/plaban/Downloads/jbwa-master/test/R2.fq"));
            ShortRead read=null;
            String S;
            while((read=kseq.next())!=null)
            {

                //System.out.println(S);

                for(AlnRgn a: mem.align(read))
                {
                    if(a.getSecondary()>=0) continue;
                    //System.out.println(  read.getName()+"\t"+  a.getStrand()+"\t"+  a.getChrom()+"\t"+ a.getPos()+"\t"+ a.getMQual()+"\t"+ a.getCigar()+"\t"+  a.getNm() );

                    String s = new String(read.getBases());
                    String s2 = new String(read.getQualities());

                    out.emit(s +"\t" + s2 + "\t" + read.getName()+"\t"+  a.getStrand()+"\t"+  a.getChrom()+"\t"+  a.getPos()+"\t"+ a.getMQual()+"\t"+ a.getCigar()+"\t"+  a.getNm());
                }
            }
            kseq.dispose();
            index.close();
            mem.dispose();*/

            /*load the index
            BwaIndex index = new BwaIndex(new File("/home/plaban/Downloads/jbwa-master/test/ref.fa"));
            //load the bwa engine
            BwaMem mem = new BwaMem(index);
            //get reads from two fastqs
            KSeq kseq1 = new KSeq(new File("/home/plaban/Downloads/jbwa-master/test/R1.fq"));
            KSeq kseq2 = new KSeq(new File("/home/plaban/Downloads/jbwa-master/test/R2.fq"));
            //build a list of two fastqs, forward and reverse
            List<ShortRead> L1 = new ArrayList<ShortRead>();
            List<ShortRead> L2 = new ArrayList<ShortRead>();
            //while something can be done
            for (; ; )
            {
                //read the pair of fastq
                ShortRead read1 = kseq1.next();
                ShortRead read2 = kseq2.next();
                //should we analyze and dump the data ?
                if (read1 == null || read2 == null || L1.size() > 100)
                {
                    if (!L1.isEmpty())
                        for (String sam : mem.align(L1, L2)) //get the SAM records
                        {
                            //System.out.println(sam);
                            out.emit(sam);
                        }
                    if (read1 == null || read2 == null) break;
                    L1.clear();
                    L2.clear();
                }
                L1.add(read1);
                L2.add(read2);
            }
            kseq1.dispose();
            kseq2.dispose();
            index.close();
            mem.dispose();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        /*System.load("/home/plaban/Downloads/jbwa-master/src/main/native/libbwajni.so");

        ShortRead shortRead = null;

        try
        {
            while ((shortRead = kSeq.next()) != null)
            {
                out.emit(shortRead);
            }
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }

        kSeq.dispose();*/

    }
}
