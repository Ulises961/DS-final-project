package org.total_order_broadcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EpochSeqNum {
    Integer currentEpoch;
    List<SeqNumValue> seqNumValueList;
    SeqNumValue lastSeqNumValueAdded;
    Map<Integer,List<SeqNumValue>> epochSeqNumValueMap;

    public EpochSeqNum(int currentEpoch, int seqNum, int value) {
        this.currentEpoch = currentEpoch;
        this.seqNumValueList = new ArrayList<>();
        lastSeqNumValueAdded = new SeqNumValue(seqNum, value);
        epochSeqNumValueMap = new HashMap<>();
        seqNumValueList.add(lastSeqNumValueAdded);
        epochSeqNumValueMap.put(currentEpoch, seqNumValueList);
    }
    public void resetSeqNum(){

    }

    public int getCurrentEpoch() {
        return currentEpoch;
    }

    public void incrementEpoch(int lastValue){
        currentEpoch+=1;
        seqNumValueList = new ArrayList<>();
        lastSeqNumValueAdded = new SeqNumValue(0,lastValue);
        seqNumValueList.add(lastSeqNumValueAdded);
        epochSeqNumValueMap.put(currentEpoch, seqNumValueList);
    }

    public SeqNumValue getLastSeqNumValue() {
        return lastSeqNumValueAdded;
    }

    public EpochSeqNum incrementSeqNum(int value) {
        lastSeqNumValueAdded = lastSeqNumValueAdded.incrementAndAddValue(value);
        seqNumValueList.add(lastSeqNumValueAdded);
        return this;
    }

    @Override
    public String toString() {
        return "EpochSeqNum{" +
                "epoch=" + currentEpoch +
                ", seqNum=" + seqNumValueList +
                '}';
    }
}
