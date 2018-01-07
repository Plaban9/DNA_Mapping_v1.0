package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.github.lindenb.jbwa.jni.BwaIndex;

import java.io.File;
import java.io.IOException;


public class FastqFilter extends BaseOperator implements Operator.ActivationListener
{
    static int counter = 0;
    //private String line;

    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

    public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>()
    {
        @Override
        public void process(String line)
        {
            filterLogic(line);
        }
    };

    @Override
    public void activate(Context context)
    {

    }

    @Override
    public void deactivate()
    {

    }

    void filterLogic(String line)
    {

        if(counter % 4 == 0 || counter % 4 == 1)
            outputPort.emit(line);

        else
            System.out.println(line);

        counter++;

    }
}
