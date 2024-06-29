package org.total_order_broadcast;

import java.util.Objects;

/**
 * Represents an epoch and sequence number combination for identification in a distributed system.
 * Implements Comparable to allow for sorting and comparison based on epoch and sequence number.
 */
public class EpochSeqNum implements Comparable<EpochSeqNum> {
    Integer currentEpoch;
    Integer seqNum;

    /**
     * Constructs an EpochSeqNum with the specified epoch and sequence number.
     *
     * @param currentEpoch The current epoch number.
     * @param seqNum The sequence number within the epoch.
     */
    public EpochSeqNum(int currentEpoch, int seqNum) {
        this.currentEpoch = currentEpoch;
        this.seqNum = seqNum;
    }

    public int getCurrentEpoch() {
        return currentEpoch;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     * Equality is based on both epoch and sequence number being equal.
     *
     * @param obj The reference object with which to compare.
     * @return true if this object is the same as the obj argument; false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EpochSeqNum esn = (EpochSeqNum) obj;
        return currentEpoch == esn.currentEpoch && seqNum == esn.seqNum;
    }

    /**
     * Compares this EpochSeqNum with another EpochSeqNum for order.
     * Comparison is first based on epoch number and then on sequence number.
     *
     * @param o The EpochSeqNum to compare to.
     * @return A negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
     */
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
