package org.total_order_broadcast;

import java.util.Objects;

public class EpochSeqNum implements Comparable<EpochSeqNum> {
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EpochSeqNum esn = (EpochSeqNum) obj;
        return currentEpoch == esn.currentEpoch && seqNum == esn.seqNum;
    }

    @Override
    public int compareTo(EpochSeqNum o) {
        if (this.currentEpoch != o.currentEpoch) {
            return Integer.compare(this.currentEpoch, o.currentEpoch);
        } else {
            return Integer.compare(this.seqNum, o.seqNum);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentEpoch, seqNum);
    }

    @Override
    public String toString() {
        return "EpochSeqNum{" +
                "epoch=" + currentEpoch +
                ", seqNum=" + seqNum +
                '}';
    }
}
