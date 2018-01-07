package com.example;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;


import java.io.*;

/**
 * Created by plaban on 6/4/17.
 */
public class FastqReader extends BaseOperator implements InputOperator
{
    private String path;
    private String line = "";
    private static int counter = 0;

    public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

    public void setPath(String path)
    {
        this.path=path;
    }

    @Override
    public void beginWindow(long windowId)
    {
        counter = 0;
    }

    public void setLine() throws IOException
    {

        FileInputStream fstream = new FileInputStream("/home/plaban/testing.fastq");
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String genome_Line;

        //Read File Line By Line
        while ((genome_Line = br.readLine()) != null)
        {
            if(counter % 3 == 0 || counter % 3 == 1)
            {
                out.emit(genome_Line);
                //this.line = genome_Line;
            }

            counter++;

        }

        fstream.close();

    }

    public String getLine()
    {
        return line;
    }

    public String getPath()
    {
        return path;
    }

    @Override
    public void emitTuples()
    {
        try
        {
            setLine();
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
