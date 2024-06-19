package org.total_order_broadcast;

public class SeqNumValue {
    public int seqNum;
    public int value;
    public SeqNumValue(int seqNum, int value) {
        this.seqNum = seqNum;
        this.value = value;
    }
    public SeqNumValue incrementAndAddValue(int value){
        return new SeqNumValue(this.seqNum+1, value);
    }
}
