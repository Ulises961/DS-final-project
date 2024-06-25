package org.total_order_broadcast;

public class EpochSeqNum {
    Integer currentEpoch;
    Integer seqNum;

    public EpochSeqNum(int currentEpoch, int seqNum) {
        this.currentEpoch = currentEpoch;
        this.seqNum = seqNum;
    }

    public int getCurrentEpoch() {
        return currentEpoch;
    }

    @Override
    public String toString() {
        return "EpochSeqNum{" +
                "epoch=" + currentEpoch +
                ", seqNum=" + seqNum +
                '}';
    }
}
