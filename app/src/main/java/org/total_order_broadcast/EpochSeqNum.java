package org.total_order_broadcast;

public class EpochSeqNum {
    int epoch;
    int seqNum;

    public EpochSeqNum(int epoch, int seqNum) {
        this.epoch = epoch;
        this.seqNum = seqNum;
    }
    public void resetSeqNum(){
        this.seqNum = 0;
    }

    public int getEpoch() {
        return epoch;
    }

    public void increaseEpoch(){
        this.epoch++;
    }
    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public void increaseSeqNum() {
        this.seqNum++;
    }
}
