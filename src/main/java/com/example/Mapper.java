package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.github.lindenb.jbwa.jni.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;


public class Mapper extends BaseOperator implements Operator.ActivationListener
{

    private BwaIndex index;
    private BwaMem mem;
    private static int counter=0;
    private String seqid;
    private byte[] seq;
    private byte[] qual;
    private ShortRead read;
    private String refName;
    @NotNull
    private String refFilePath;
    private String outputFilePath;
    private Boolean isValidPath = true;

    private static final Logger LOG = LoggerFactory.getLogger(Mapper.class);

    public final transient DefaultOutputPort<String>  outputPort= new DefaultOutputPort<>();

    public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>()
    {
        @Override
        public void process(String line)
        {
            switch(counter%4)
            {
                case 0: seqid = line;
                        ++counter;
                        break;

                case 1: seq = line.getBytes();
                        ++counter;
                        break;

                case 2: ++counter;
                        break;

                case 3: qual = line.getBytes();
                        read = new ShortRead(seqid, seq, qual);
                        ++counter;

                        try
                        {
                            for (AlnRgn a : mem.align(read))
                            {
                                if (a.getSecondary() >= 0)
                                    continue;

                                String sequence = new String(read.getBases());
                                String qual = new String(read.getQualities());
                                outputPort.emit(read.getName() + "\t" + a.getAs() + "\t" +  a.getChrom() +  "\t" + a.getPos() + "\t" + a.getMQual() + "\t" + a.getCigar() + "\t" + "*" + "\t" + 0 + "\t" + 0 + "\t" + sequence + "\t" + qual +  "\tNM:i:" +  a.getNm() );
                            }
                        }

                        catch (IOException e)
                        {
                            e.printStackTrace();
                        }

                        break;
            }
        }
    };


    @Override
    public void activate(Context context)
    {
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
                LOG.info("Time elapsed in loading genome: " + ( stop.getTime() - start.getTime()) + "ms");
                LOG.info("Beginning to map reads to reference genome...");
            }

            else
            {
                isValidPath = false;
                LOG.error("Error!! Invalid Path: " + getRefFilePath());
            }


            /*BufferedReader reader = new BufferedReader(new FileReader(refFile));
            refName = reader.readLine();

            if (refName.charAt(0) == '>')
                refName = refName.substring( 1, refName.length());*/
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

    @Override
    public void deactivate()
    {
        if (isValidPath)
            LOG.info("Teardown of Mapping operation.........Started");

        index.close();
        mem.dispose();

        if (isValidPath)
            LOG.info("Teardown of Mapping operation........Finished");

        else
            LOG.info("Mapping operation unsuccessful.......File not found at " + getRefFilePath());
    }
}
