package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.KSeq;
import com.github.lindenb.jbwa.jni.ShortRead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PairMapper extends BaseOperator implements InputOperator
{

    private BwaIndex index;
    private BwaMem mem;
    private KSeq fastq_1;
    private KSeq fastq_2;
    private ShortRead read;
    private String refName;
    @NotNull
    private String refFilePath;
    @NotNull
    private String fastqPath_1;
    @NotNull
    private String fastqPath_2;
    private String outputFilePath;
    private Boolean isValidRefPath = true;

    private static final Logger LOG = LoggerFactory.getLogger(Mapper.class);

    public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

    @Override
    public void emitTuples()
    {
        /*List<ShortRead> L1 = new ArrayList<ShortRead>();
        List<ShortRead> L2 = new ArrayList<ShortRead>();
        //while something can be done
        //read the pair of fastq
        ShortRead read1 = fastq_1.next();
        ShortRead read2 = fastq_2.next();
        //should we analyze and dump the data ?
        if (read1 == null || read2 == null || L1.size() > 100)
        {
            if (!L1.isEmpty())
                for (String sam : mem.align(L1, L2)) //get the SAM records
                {
                    //System.out.println(sam);
                    out.emit(sam);
                }

            if (read1 == null || read2 == null)
                break;

            L1.clear();
            L2.clear();

        }

        L1.add(read1);
        L2.add(read2);*/

    }


    @Override
    public void setup(Context.OperatorContext context)
    {
        super.setup(context);

        System.load("/home/plaban/Downloads/jbwa-master/src/main/native/libbwajni.so");

        try
        {
            LOG.info("Loading Reference Genome and its Indices......");

            Date start = new Date();
            File refFile = new File(getRefFilePath());

            if (refFile.isFile())
            {
                index = new BwaIndex(refFile);
                mem = new BwaMem(index);

                Date stop = new Date();
                LOG.info("Time elapsed in loading genome: " + (stop.getTime() - start.getTime()) + "ms");
                LOG.info("Beginning to map reads to reference genome...");
            }

            else
            {
                isValidRefPath = false;
                LOG.error("Error!! Invalid Path: " + getRefFilePath());
            }

            fastq_1= new KSeq(new File(fastqPath_1));
            fastq_2= new KSeq(new File(fastqPath_2));
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

        public String getRefFilePath()
        {
            return refFilePath;
        }

        public void setRefFilePath(String path)
        {
            this.refFilePath = path;
        }
}
