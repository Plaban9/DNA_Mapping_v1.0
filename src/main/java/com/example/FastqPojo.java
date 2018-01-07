package com.example;

public class FastqPojo
{
    private String sequenceIdentifier;
    private String sequence;

    public void setSequenceIdentifier(String sequenceIdentifier)
    {
        this.sequenceIdentifier = sequenceIdentifier;
    }

    public void setSequence(String sequence)
    {
        this.sequence = sequence;
    }

    public String getSequenceIdentifier()
    {
        return sequenceIdentifier;
    }

    public String getSequence()
    {
        return sequence;
    }

}
