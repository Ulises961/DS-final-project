package org.total_order_broadcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
