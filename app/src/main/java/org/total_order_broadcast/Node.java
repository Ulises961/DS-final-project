package org.total_order_broadcast;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.io.Serializable;

import scala.concurrent.duration.Duration;

import akka.actor.*;

public abstract class Node extends AbstractActor {
    protected int id;                           // node ID
    protected List<ActorRef> participants;      // list of participant nodes
    protected Decision decision = null;// decision taken by this node

    protected int quorum;

    // dedicated class to keep track of epoch and seqNum pairs :)
    private EpochSeqNum epochSeqNumPair;


    // whether the node should join through the manager
    public boolean isCoordinator;

    // participants (initial group, current and proposed views)
    private final Set<ActorRef> group;
    private final Set<ActorRef> currentView;

    // each view has is assocaited w/ an Epoch
    private final Map<EpochSeqNum, Set<ActorRef>> proposedView;


    // last sequence number for each node message (to avoid delivering duplicates)
    private final Map<ActorRef, Integer> membersSeqno;

    // unstable messages
    private final Set<ChatMsg> unstableMsgSet;

    // deferred messages (of a future view)
    private final Set<ChatMsg> deferredMsgSet;

    // group view flushes
    private final Map<Integer, Set<ActorRef>> flushes;

    private final Random rnd;

    // type of the next simulated crash
    enum CrashType {
        NONE,
        ChatMsg,
        StableChatMsg,
        ViewFlushMsg
    }
    private CrashType nextCrash;

    // number of transmissions before crashing
    private int nextCrashAfter;

    // prepare to recover (to handle RecoveryMsg received before crashing)
    private boolean willRecover;

    public Node(int id, boolean isCoordinator) {
        super();
        this.id = id;
        this.epochSeqNumPair = new EpochSeqNum(0,0);
        this.isCoordinator = isCoordinator;
        this.group = new HashSet<>();
        this.currentView = new HashSet<>();
        this.proposedView = new HashMap<>();
        this.membersSeqno = new HashMap<>();
        this.unstableMsgSet = new HashSet<>();
        this.deferredMsgSet = new HashSet<>();
        this.flushes = new HashMap<>();
        this.rnd = new Random();
        this.nextCrash = CrashType.NONE;
        this.nextCrashAfter = 0;
        this.willRecover = false;
        this.quorum = (N_PARTICIPANTS/2) + 1;
    }

    private void updateQuorum(){
        this.quorum = currentView.size()/2 +1;
    }
    final static int N_PARTICIPANTS = 3;
    final static int VOTE_TIMEOUT = 1000;      // timeout for the votes, ms
    final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms

    // the votes that the participants will send (for testing)
    final static Vote[] predefinedVotes = 
        new Vote[] {Vote.YES, Vote.YES, Vote.YES}; // as many as N_PARTICIPANTS

    // Start message that sends the list of participants to everyone
    public static class StartMessage implements Serializable {
        public final List<ActorRef> group;
        public StartMessage(List<ActorRef> group) {
        this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    public enum Vote {NO, YES}
    public enum Decision {ABORT, COMMIT}

    public static class VoteRequest implements Serializable {}

    public static class VoteResponse implements Serializable {
        public final Vote vote;
        public VoteResponse(Vote v) { vote = v; }
    }

    public static class DecisionRequest implements Serializable {}

    public static class DecisionResponse implements Serializable {
        public final Decision decision;
        public DecisionResponse(Decision d) { decision = d; }
    }

    public static class Timeout implements Serializable {}

    public static class Recovery implements Serializable {}
     public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> group;   // an array of group members
    public JoinGroupMsg(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
    }
  }

  public static class SendChatMsg implements Serializable {}

  public static class ChatMsg implements Serializable {
    public final Integer viewId;
    public final ActorRef sender;
    public final Integer seqno;
    public final String content;
    public ChatMsg(int viewId, ActorRef sender, int seqno, String content) {
      this.viewId = viewId;
      this.sender = sender;
      this.seqno = seqno;
      this.content = content;
    }
  }

  public static class StableChatMsg implements Serializable {
    public final ChatMsg stableMsg;
    public StableChatMsg(ChatMsg stableMsg) {
      this.stableMsg = stableMsg;
    }
  }

  public static class StableTimeoutMsg implements Serializable {
    public final ChatMsg unstableMsg;
    public final ActorRef sender;
    public StableTimeoutMsg(ChatMsg unstableMsg, ActorRef sender) {
      this.unstableMsg = unstableMsg;
      this.sender = sender;
    }
  }

  public static class ViewChangeMsg implements Serializable {
    public final Integer viewId;
    public final Set<ActorRef> proposedView;
    public ViewChangeMsg(int viewId, Set<ActorRef> proposedView) {
      this.viewId = viewId;
      this.proposedView = Collections.unmodifiableSet(new HashSet<>(proposedView));
    }
  }

  public static class ViewFlushMsg implements Serializable {
    public final Integer viewId;
    public ViewFlushMsg(int viewId) {
      this.viewId = viewId;
    }
  }

  public static class FlushTimeoutMsg implements Serializable {
    public final Integer viewId;
    public FlushTimeoutMsg(int viewId) {
      this.viewId = viewId;
    }
  }

  public static class CrashMsg implements Serializable {
    public final CrashType nextCrash;
    public final Integer nextCrashAfter;
    public CrashMsg(CrashType nextCrash, int nextCrashAfter) {
      this.nextCrash = nextCrash;
      this.nextCrashAfter = nextCrashAfter;
    }
  }

  public static class RecoveryMsg implements Serializable {}


    // abstract method to be implemented in extending classes
    protected abstract void onRecovery(Recovery msg);

    void setGroup(StartMessage sm) {
      participants = new ArrayList<>();
      for (ActorRef b: sm.group) {
        if (!b.equals(getSelf())) {

          // copying all participant refs except for self
          this.participants.add(b);
        }
      }
      print("starting with " + sm.group.size() + " peer(s)");
    }

    // emulate a crash and a recovery in a given time
    void crash(int recoverIn) {
      getContext().become(crashed());
      print("CRASH!!!");

      // setting a timer to "recover"
      getContext().system().scheduler().scheduleOnce(
          Duration.create(recoverIn, TimeUnit.MILLISECONDS),  
          getSelf(),
          new Recovery(), // message sent to myself
          getContext().system().dispatcher(), getSelf()
          );
    }

    // emulate a delay of d milliseconds
    void delay(int d) {
      try {Thread.sleep(d);} catch (Exception ignored) {}
    }

    void multicast(Serializable m) {
      for (ActorRef p: participants)
        p.tell(m, getSelf());
    }

    // a multicast implementation that crashes after sending the first message
    void multicastAndCrash(Serializable m, int recoverIn) {
      for (ActorRef p: participants) {
        p.tell(m, getSelf());
        crash(recoverIn); return;
      }
    }

    // schedule a Timeout message in specified time
    void setTimeout(int time) {
      getContext().system().scheduler().scheduleOnce(
          Duration.create(time, TimeUnit.MILLISECONDS),  
          getSelf(),
          new Timeout(), // the message to send
          getContext().system().dispatcher(), getSelf()
          );
    }

    // fix the final decision of the current node
    void fixDecision(Decision d) {
      if (!hasDecided()) {
        this.decision = d;
        print("decided " + d);
      }
    }

    boolean hasDecided() { return decision != null; } // has the node decided?

    // a simple logging function
    void print(String s) {
      System.out.format("%2d: %s\n", id, s);
    }

    @Override
    public Receive createReceive() {

      // Empty mapping: we'll define it in the inherited classes
      return receiveBuilder().build();
    }

    public Receive crashed() {
      return receiveBuilder()
              .match(Recovery.class, this::onRecovery)
              .matchAny(msg -> {})
              .build();
    }

    public void onDecisionRequest(DecisionRequest msg) {  /* Decision Request */
      if (hasDecided()) {
        getSender().tell(new DecisionResponse(decision), getSelf());
          String message = "received decision request from " +
                  getSender() +
                  decision.toString();
        print(message);
      }
      // just ignoring if we don't know the decision
    }
    public ActorRef getCoordinator(){
        for (ActorRef a: participants){
      //TODO : create a method that returns the coordinator
        }
        return null;
    }
  }