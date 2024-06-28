package org.total_order_broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import scala.util.Random;

public abstract class Node extends AbstractActor {

    protected int id; // node ID

    protected int quorum;

    protected Integer currentValue;

    // dedicated class to keep track of epoch and seqNum pairs :)
    protected EpochSeqNum epochSeqNumPair;

    // whether the node should join through the manager
    protected ActorRef coordinator;

    // participants (initial group, current and proposed views)
    protected final Set<ActorRef> group;

    protected Set<ActorRef> currentView;

    // each view is associated w/ an Epoch
    protected final Map<Integer, Set<ActorRef>> proposedView;

    // last sequence number for each node message (to avoid delivering duplicates)
    protected final Map<ActorRef, Integer> membersSeqno;

    // history of updates
    protected Map<EpochSeqNum, Integer> updateHistory;

    // deferred messages (of a future view)
    protected final Set<WriteDataMsg> deferredMsgSet;

    // cancellation of the heartbeat timeout
    protected Cancellable heartbeatTimeout;

    // next heartbeat
    protected Cancellable heartbeat;

    protected Cancellable voteTimeout;

    protected Cancellable electionTimeout;

    // to include delays in the messages
    private Random rnd = new Random();

    protected Map<Integer, Set<ActorRef>> flushes;

    // Hearbeat
    final static int HEARTBEAT_TIMEOUT_DURATION = 4000;
    final static int HEARTBEAT_INTERVAL = 2000;
    final static int VOTE_TIMEOUT = 5000;
    final static int N_PARTICIPANTS = 7;
    final static int MAX_DELAY = 500; // max network delay, ms
    final static int READ_TIMEOUT = 500; // timeout to respond to a client, ms
    final static int DECISION_TIMEOUT = 1000; // timeout for the decision, ms
    final static int ELECTION_TIMEOUT = 4000; // timeout for the election, ms

    protected Logger logger;

    protected Map<String, String> contextMap;

    public Node(int id) {
        super();
        this.id = id;
        this.epochSeqNumPair = new EpochSeqNum(0, 0);
        this.group = new HashSet<>();
        this.currentView = new HashSet<>();
        this.proposedView = new HashMap<>();
        this.membersSeqno = new HashMap<>();
        this.deferredMsgSet = new HashSet<>();
        this.quorum = (N_PARTICIPANTS / 2) + 1;
        updateHistory = new TreeMap<>();
        updateHistory.put(epochSeqNumPair, 0);
        logger = LoggerFactory.getLogger(Node.class);
    }

    protected void updateQuorum(int n) {
        this.quorum = n / 2 + 1;
    }


    /**
     * Message class used to initiate a group of actors with a coordinator.
     * Contains an immutable list of group members and the coordinator actor reference.
     */
    public static class StartMessage implements Serializable {
        public final List<ActorRef> group;
        public final ActorRef coordinator;

        /**
         * Constructs a StartMessage with an unmodifiable list of group members and the coordinator.
         *
         * @param group The list of actors forming the group.
         * @param coordinator The coordinator actor reference.
         */
        public StartMessage(List<ActorRef> group, ActorRef coordinator) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
            this.coordinator = coordinator;
        }
    }

    /**
     * Empty serializable class used to signal the crash of the coordinator.
     */
    public static class CrashCoord implements Serializable {
        // nothing here
    }

    /**
     * Message class used to request an update with a specific value and epoch sequence number.
     */
    public static class UpdateRequest implements Serializable {
        public final Integer value;
        public final EpochSeqNum epochSeqNum;

        /**
         * Constructs an UpdateRequest with the specified value and epoch sequence number.
         *
         * @param value The integer value to update.
         * @param epochSeqNum The epoch sequence number associated with the update.
         */
        public UpdateRequest(Integer value, EpochSeqNum epochSeqNum) {
            this.value = value;
            this.epochSeqNum = epochSeqNum;
        }
    }

    /**
     * Message class used to synchronize update history between nodes.
     * Contains an unmodifiable map of epoch sequence numbers to integer values.
     */
    public static class SyncMessage implements Serializable {
        public Map<EpochSeqNum, Integer> updateHistory;

        /**
         * Constructs a SyncMessage with an unmodifiable map of update history.
         *
         * @param updateHistory The map containing epoch sequence numbers mapped to integer values.
         */
        public SyncMessage(Map<EpochSeqNum, Integer> updateHistory) {
            this.updateHistory = Collections.unmodifiableMap(new HashMap<EpochSeqNum, Integer>(updateHistory));
        }
    }

    /**
     * Message class used to acknowledge receipt of an update request with a specific value and epoch sequence number.
     */
    public static class UpdateAck implements Serializable {
        public final Integer value;
        public final EpochSeqNum epochSeqNum;

        /**
         * Constructs an UpdateAck with the acknowledged value and epoch sequence number.
         *
         * @param val The acknowledged integer value.
         * @param epochSeqNum The epoch sequence number associated with the update.
         */
        public UpdateAck(Integer val, EpochSeqNum epochSeqNum) {
            value = val;
            this.epochSeqNum = epochSeqNum;
        }
    }

    /**
     * Message class used to confirm the successful completion of a write operation (WriteOk).
     * Contains the value, epoch sequence number, and proposer actor reference.
     */
    public static class WriteOk implements Serializable {
        public final Integer value;
        public final EpochSeqNum epochSeqNum;
        public final ActorRef proposer;

        /**
         * Constructs a WriteOk message with the confirmed value, epoch sequence number, and proposer.
         *
         * @param v The confirmed integer value.
         * @param epochSeqNum The epoch sequence number associated with the write operation.
         * @param proposer The actor reference of the proposer initiating the write operation.
         */
        public WriteOk(Integer v, EpochSeqNum epochSeqNum, ActorRef proposer) {
            value = v;
            this.epochSeqNum = epochSeqNum;
            this.proposer = proposer;
        }
    }

    /**
     * Marker interface for timeout-related messages.
     * Used as a base class for specific timeout message types.
     */
    public static class Timeout implements Serializable {
    }

    /**
     * Message class indicating a timeout for update operations with a specific epoch sequence number.
     * Extends Timeout.
     */
    public static class UpdateTimeOut extends Timeout {
        public final EpochSeqNum epochSeqNum;

        /**
         * Constructs an UpdateTimeOut message with the specified epoch sequence number.
         *
         * @param esn The epoch sequence number associated with the update timeout.
         */
        public UpdateTimeOut(EpochSeqNum esn) {
            epochSeqNum = esn;
        }
    }

    /**
     * Empty serializable class used to indicate recovery-related messages.
     * Used for signaling recovery processes or events.
     */
    public static class Recovery implements Serializable {
    }

    /**
     * Message class used to request joining a group of actors.
     * Contains an unmodifiable list of group members, coordinator, and supervisor actor references.
     */
    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group; // an array of group members
        public final ActorRef coordinator; // the coordinator
        public final ActorRef supervisor; // the supervisor gets messages directly to the coordinator

        /**
         * Constructs a JoinGroupMsg with an unmodifiable list of group members, coordinator, and supervisor.
         *
         * @param group The list of actors forming the group.
         * @param coordinator The coordinator actor reference.
         * @param supervisor The supervisor actor reference.
         */
        public JoinGroupMsg(List<ActorRef> group, ActorRef coordinator, ActorRef supervisor) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
            this.coordinator = coordinator;
            this.supervisor = supervisor;
        }
    }

    /**
     * Message class used to request reading data from an actor.
     * Contains the sender actor reference.
     */
    public static class ReadDataMsg implements Serializable {
        ActorRef sender;

        /**
         * Constructs a ReadDataMsg with the sender actor reference.
         *
         * @param sender The actor reference of the message sender.
         */
        public ReadDataMsg(ActorRef sender) {
            this.sender = sender;
        }
    }

    /**
     * Message class containing data to be sent between actors.
     */
    public static class DataMsg implements Serializable {
        Integer value;

        /**
         * Constructs a DataMsg with the specified integer value.
         *
         * @param value The integer value to be sent.
         */
        public DataMsg(Integer value) {
            this.value = value;
        }
    }

    /**
     * Empty serializable class used to acknowledge election-related messages.
     * Used as a marker for acknowledging election processes or events.
     */
    public static class ElectionAck implements Serializable {
    }

    /**
     * Message class indicating a heartbeat signal.
     * Extends Timeout and serves as a marker for heartbeat messages.
     */
    public static class Heartbeat extends Timeout {
    }

    /**
     * Message class indicating a timeout for heartbeat signaling.
     * Extends Timeout and serves as a marker for heartbeat timeout messages.
     */
    public static class HeartbeatTimeout extends Timeout {
    }

    /**
     * Message class indicating a timeout for election-related operations.
     * Extends Timeout and includes the next election phase and active replicas information.
     */
    public static class ElectionTimeout extends Timeout {
        int next;
        Map<Integer, Set<ActorRef>> activeReplicas;

        /**
         * Constructs an ElectionTimeout message with the specified next phase and active replicas map.
         *
         * @param next The next phase of the election.
         * @param activeReplicas The map of active replicas participating in the election.
         */
        public ElectionTimeout(int next, Map<Integer, Set<ActorRef>> activeReplicas) {
            this.next = next;
            this.activeReplicas = activeReplicas;
        }
    }

    /**
     * Message class used to signal a restart of the election process.
     * Indicates that a new election should be initiated.
     */
    public static class RestartElection implements Serializable {
    }

    /**
     * Message class used to signal the successful restart of the election process.
     * Indicates that the election process has been successfully restarted.
     */
    public static class ElectionRestated implements Serializable {
    }

    /**
     * Message class used to send a write operation request with a value and optional crash flag.
     */
    public static class WriteDataMsg implements Serializable {
        public final Integer value;
        public final boolean shouldCrash;
        ActorRef sender;

        /**
         * Constructs a WriteDataMsg with the specified value, sender, and crash flag.
         *
         * @param value The integer value to write.
         * @param sender The actor reference of the message sender.
         * @param shouldCrash Boolean flag indicating whether the sender should crash after sending the message.
         */
        public WriteDataMsg(Integer value, ActorRef sender, boolean shouldCrash) {
            this.value = value;
            this.sender = sender;
            this.shouldCrash = shouldCrash;
        }
        /**
         * Constructs a WriteDataMsg with the specified value and sender.
         * Defaults the shouldCrash flag to false.
         *
         * @param value The integer value to write.
         * @param sender The actor reference of the message sender.
         */
        public WriteDataMsg(Integer value, ActorRef sender) {
            this.value = value;
            this.sender = sender;
            this.shouldCrash = false;
        }
    }

    /**
     * Specialized message class representing a pending write operation.
     * Extends WriteDataMsg with constructors for pending writes.
     */
    public static class PendingWriteMsg extends WriteDataMsg {
        /**
         * Constructs a PendingWriteMsg with the specified value, sender, and crash flag.
         *
         * @param value The integer value to write.
         * @param sender The actor reference of the message sender.
         * @param shouldCrash Boolean flag indicating whether the sender should crash after sending the message.
         */
        public PendingWriteMsg(Integer value, ActorRef sender, boolean shouldCrash) {
            super(value, sender, shouldCrash);
        }

        /**
         * Constructs a PendingWriteMsg with the specified value and sender.
         * Defaults the shouldCrash flag to false.
         *
         * @param value The integer value to write.
         * @param sender The actor reference of the message sender.
         */
        public PendingWriteMsg(Integer value, ActorRef sender) {
            super(value, sender);
        }
    }

    /**
     * Message class used to communicate a view change event.
     * Contains the epoch sequence number, proposed view, and coordinator actor reference.
     */
    public static class ViewChangeMsg implements Serializable {
        public final EpochSeqNum esn;
        public final Set<ActorRef> proposedView;
        public final ActorRef coordinator;

        /**
         * Constructs a ViewChangeMsg with the specified epoch sequence number, proposed view, and coordinator.
         *
         * @param esn The epoch sequence number associated with the view change.
         * @param proposedView The set of proposed view members.
         * @param coordinator The coordinator actor reference.
         */
        public ViewChangeMsg(EpochSeqNum esn, Set<ActorRef> proposedView, ActorRef coordinator) {
            this.esn = esn;
            this.proposedView = Collections.unmodifiableSet(new HashSet<>(proposedView));
            this.coordinator = coordinator;
        }
    }

    /**
     * Empty serializable class used to signal a crash event.
     * Serves as a marker for crash-related messages or events.
     */
    public static class CrashMsg implements Serializable {
    }

    public static class ICMPRequest implements Serializable {
    }

    public static class ICMPResponse implements Serializable {
    }

    public static class FlushMsg implements Serializable {
    }

    public static class ICMPTimeout extends Timeout {
    }

    /**
     * Message class used to notify an update with a specific epoch sequence number.
     */
    public static class UpdateMsg implements Serializable {
        public final EpochSeqNum epochSeqNum;

        /**
         * Constructs an UpdateMsg with the specified epoch sequence number.
         *
         * @param esn The epoch sequence number associated with the update.
         */
        public UpdateMsg(EpochSeqNum esn) {
            this.epochSeqNum = esn;
        }
    }

    /**
     * Message class used to signal the completion of a flush operation for a specific epoch.
     * Contains the epoch number for which the flush operation has completed.
     */
    public static class FlushCompleteMsg implements Serializable {
        public final int epoch;

        /**
         * Constructs a FlushCompleteMsg with the specified epoch number.
         *
         * @param epoch The epoch number for which the flush operation has completed.
         */
        public FlushCompleteMsg(int epoch) {
            this.epoch = epoch;
        }
    }

    public static class ReadHistory implements Serializable {
    }

    public void setValue(int value) {
        this.currentValue = value;
    }

    public int getValue() {
        return currentValue != null ? currentValue : 0;
    }

    // abstract method to be implemented in extending classes
    protected abstract void onRecovery(Recovery msg);

    /**
     * Sets the current group with the specified JoinGroupMsg.
     * Adds each actor in the group to the current view.
     *
     * @param sm The JoinGroupMsg containing the group members and coordinator.
     */
    public void setGroup(JoinGroupMsg sm) {
        for (ActorRef b : sm.group) {
            currentView.add(b);
        }

        log("Starting with " + sm.group.size() + " peer(s)", Cluster.LogLevel.INFO);
        log("Participants: " + currentView.toString(), Cluster.LogLevel.INFO);
    }

    /**
     * Simulates a crash by changing the actor's behavior to a crashed state.
     * Logs the crash event.
     */
    public void crash() {
        getContext().become(crashed());
        log("CRASH!!!", Cluster.LogLevel.ERROR);
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> log("Ignoring " + msg.getClass().getSimpleName() + " (crashed)", Cluster.LogLevel.DEBUG))
                .build();
    }

    /**
     * Emulates a delay of d milliseconds.
     *
     * @param d The duration in milliseconds to delay.
     */
    public static void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (Exception ignored) {
        }
    }

    /**
     * Multicasts a message m to all actors in the current view with random delays.
     *
     * @param m The Serializable message to multicast.
     */
    public void multicast(Serializable m) {
        for (ActorRef p : currentView) {
            int randomDelay = randomDelay();
            delay(randomDelay);
            p.tell(m, getSelf());

        }
    }

    /**
     * Multicasts a message m to all actors in the current view except for a specified actor, with random delays.
     *
     * @param m The Serializable message to multicast.
     * @param except The ActorRef of the actor to exclude from multicast.
     */
    public void multicastExcept(Serializable m, ActorRef except) {
        for (ActorRef p : currentView) {
            if (!p.equals(except)) {
                int randomDelay = randomDelay();
                delay(randomDelay);
                p.tell(m, getSelf());
            }
        }
    }

    /**
     * Multicasts a message m to all actors in the current view with random delays,
     * and crashes the actor after sending the first message.
     *
     * @param m The Serializable message to multicast.
     */
    public void multicastAndCrash(Serializable m) {
        for (ActorRef p : currentView) {
            int randomDelay = randomDelay();
            delay(randomDelay);
            p.tell(m, getSelf());
            crash();
            return;
        }
    }

    /**
     * Schedules a Timeout message to be sent to the actor after a specified time.
     *
     * @param time The time in milliseconds after which the timeout should occur.
     * @param timeout The Serializable message to send as a timeout.
     * @return A Cancellable object representing the scheduled timeout task.
     */
    Cancellable setTimeout(int time, Serializable timeout) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                timeout, // the message to send
                getContext().system().dispatcher(), getSelf());
    }

    /**
     * Fixes the final decision of the current node by committing a value to the state.
     * Logs the update and updates the node's state with the given epoch sequence number.
     *
     * @param v The integer value to commit.
     * @param epochSeqNum The {@code EpochSeqNum} associated with the commit decision.
     */
    public void commitDecision(Integer v, EpochSeqNum epochSeqNum) {
        if (!hasDecided(epochSeqNum)) {
            currentValue = v;
            updateHistory.put(epochSeqNum, v);
            epochSeqNumPair = epochSeqNum;
            log("update " + epochSeqNumPair.getCurrentEpoch() + ":" + epochSeqNumPair.seqNum + " " + currentValue, Cluster.LogLevel.INFO);
            log("Update History " + updateHistory, Cluster.LogLevel.DEBUG);
        }
    }

    /**
     * Checks if the node has already decided on a commit decision for a specific epoch sequence number.
     *
     * @param epochSeqNum The {@code EpochSeqNum} to check for decision.
     * @return True if the node has already decided (the epoch sequence number exists in updateHistory); false otherwise.
     */
    boolean hasDecided(EpochSeqNum epochSeqNum) {
        return this.updateHistory.get(epochSeqNum) != null;
    } // has the node decided?

    // a simple logging function
    public void print(String s) {
        System.out.format("%2d: %s\n", id, s);
    }

    @Override
    public Receive createReceive() {

        // Empty mapping: we'll define it in the inherited classes
        return receiveBuilder().build();
    }

    /**
     * Utility method for logging messages with a specified log level and MDC context.
     *
     * @param message The message to log.
     * @param level The {@code LogLevel} indicating the severity of the log message.
     */
    protected void log(String message, Cluster.LogLevel level) {
        Cluster.logWithMDC(message, contextMap, logger, level);
    }

    /**
     * Generates a random delay based on the current configuration.
     * Uses the formula: upperbound = Math.round((MAX_DELAY + 50) / N_PARTICIPANTS)
     *
     * @return An integer representing the randomly generated delay.
     */
    protected int randomDelay() {
        int upperbound = Math.round((MAX_DELAY + 50) / N_PARTICIPANTS); // random delay
        return rnd.nextInt(upperbound);
    }
}