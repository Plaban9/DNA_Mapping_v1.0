/**
 * Put your copyright and license info here.
 */
package com.example;


import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;


@ApplicationAnnotation(name="DNA_Mapping")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
      LineByLineFileInputOperator fastqReader = dag.addOperator("fastqReader",LineByLineFileInputOperator.class);
      //FastqReader fastqReader = dag.addOperator("FastqReader",FastqReader.class);
      //GenericFileOutputOperator.StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", GenericFileOutputOperator.StringFileOutputOperator.class);
      //FastqFilter fastqFilter = dag.addOperator("test",FastqFilter.class);
      //fastqReader.setDirectory("/home/plaban/Test/Input");
      //FSRecordReaderModule fsRecordReaderModule=dag.addModule("freader",FSRecordReaderModule.class);

      //dag.addStream("toTest",fsRecordReaderModule.records,fastqFilter.input);
      //dag.addStream("totest", fastqReader.output, fastqFilter.input);

      //FSRecordReaderModule fastqReader = dag.addModule("fastqReader", FSRecordReaderModule.class);
      //fastqReader.setFiles("file:///home/plaban/Test/Input/Fastq_Test.fastq");
      //AbstractFileInputOperator.DirectoryScanner directoryScanner = new AbstractFileInputOperator.DirectoryScanner();
      //directoryScanner.scan();
      //directoryScanner.setFilePatternRegexp("Fastq_Test.fastq");


      //FastqFilter fastqFilter = dag.addOperator("fastqFilter", FastqFilter.class);
      //Mapper alignment = dag.addOperator("alignment", Mapper.class);

      //FastqReaderSender fastqReaderSender =dag.addOperator("fastqSender",FastqReaderSender.class);
      GenericFileOutputOperator.StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", GenericFileOutputOperator.StringFileOutputOperator.class);
      //fastqReaderSender.setFastqPath("/home/plaban/Downloads/jbwa-master/test/R1.fq");

      Mapper mapper =dag.addOperator("mapper", Mapper.class);

      //ConsoleOutputOperator consoleOutputOperator = dag.addOperator("consoleOutputOperator", ConsoleOutputOperator.class);

      //dag.addStream("toFilter", fastqReader.records, fastqFilter.inputPort);
     // dag.addStream("toFilter", fastqReader.output, fastqFilter.inputPort);
        //dag.addStream("toMapper", fastqReader.out, alignment.inputPort);
        //dag.addStream("toFileOutput", fastqReaderSender.out,fileOutput.input);

      //dag.addStream("toAligner", fastqFilter.outputPort, alignment.inputPort);
      //dag.addStream("toconsoleOutputOperator", alignment.outputPort,consoleOutputOperator.input);
      //dag.addStream("toFileSystem", fastqFilter.outputPort, fileOutput.input);

      //LineByLineFileInputOperator fastqReader = dag.addOperator("fastqReader", LineByLineFileInputOperator.class);
      //Mapper mapper = dag.addOperator("Mapper", Mapper.class);


      dag.addStream("toMapper", fastqReader.output, mapper.inputPort);
      dag.addStream("toSAMWriter", mapper.outputPort, fileOutput.input);
  }
}
