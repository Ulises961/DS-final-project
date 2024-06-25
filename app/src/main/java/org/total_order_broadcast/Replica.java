package org.total_order_broadcast;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;



public class Replica extends Node {
  HashSet<ActorRef> receivedAcks;
  private ActorRef proposedCoord = null;
  private int proposedCoordinatorID;
  private boolean expectingAcks = false;
  private boolean nextHopTimedOut = false;
  private int nextHop = -1;
  private boolean hasReceivedElectionMessage = false;
  private boolean deferringMessages = false;
  private Integer currentRequest = 0;
  
  // Replica requests that have not been yet put to vote
  private List<WriteDataMsg> pendingMsg;
  
  // Unordered writes remain pending until the sequence is correct
  private Map<EpochSeqNum, WriteOk> unstableWrites;
  
  // Coordinator keeps track of pending requests associated to a Replica
  private Map<EpochSeqNum, ActorRef> pendingRequests;
  
  private Cancellable updateTimeOut;
  
  public Replica(int id) {
    super(id);
    this.receivedAcks = new HashSet<>();
    this.pendingRequests = new HashMap<>();
    this.pendingMsg = new LinkedList<>();
    this.unstableWrites = new HashMap<>();
    logger = LoggerFactory.getLogger(Replica.class);

    contextMap = new HashMap<>();
    contextMap.put("replicaId", String.valueOf(id));

    System.out.println("Replica " + id + " created");
  }

  @Override
  protected void onRecovery(Recovery msg) {
    return; // An Actor needs to implement an onRecovery method./
  }

  static public Props props(int id) {
    return Props.create(Replica.class, () -> new Replica(id));
  }

  public void onCrash(CrashMsg msg){
    crash();
  }
  // election message, this is what each replica receives/sends
  public static class ElectionMessage implements Serializable {
    public ActorRef proposedCoordinator;
    Map<EpochSeqNum, Integer> updateHistory;
    int proposedCoordinatorID;
    // group view flushes
    protected final Map<Integer, Set<ActorRef>> flushes;

    public ElectionMessage(Map<EpochSeqNum, Integer> updateHistory, ActorRef coord, int proposedCoordinatorID, Map<Integer, Set<ActorRef>> flushes) {
      this.updateHistory = updateHistory;
      this.proposedCoordinatorID = proposedCoordinatorID;
      this.proposedCoordinator = coord;
      this.flushes = flushes;
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onStartMessage)
        .match(UpdateTimeOut.class, this::onTimeout)
        .match(Recovery.class, this::onRecovery)
        .match(WriteDataMsg.class, this::onUpdateMessage)
        .match(ReadDataMsg.class, this::onReadMessage)
        .match(UpdateRequest.class, this::onUpdateRequest)
        .match(UpdateAck.class, this::onUpdateAck)
        .match(WriteOk.class, this::onWriteOk)
        .match(Heartbeat.class, this::onHeartbeat)
        .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(ICMPRequest.class, this::onPing)
        .match(CrashMsg.class, this::onCrash)
        .build();
      }
      
    public Receive electionMode() {
      return receiveBuilder()
      .match(ElectionTimeout.class, this::onElectionTimeout)
      .match(ElectionAck.class, this::onElectionAck)
      .match(ElectionMessage.class, this::onElectionMessageReceipt)
      .match(WriteDataMsg.class, this::onUpdateMessage)
      .match(PendingWriteMsg.class, this::onPendingWriteMessage)
      .match(FlushMsg.class, this::onFlushMessage)
      .match(SyncMessage.class, this::onSyncMessageReceipt)
      .match(ViewChangeMsg.class, this::onViewChange)
      .matchAny(msg -> System.out.println(getSelf().path().name() + " ignoring " + msg.getClass().getSimpleName() + " (crashed)"))
      .build();
  }
  
  public void onStartMessage(JoinGroupMsg msg) {
    setGroup(msg);
    this.coordinator = msg.coordinator;
    if (isCoordinator()) {
      multicast(new Heartbeat());
    }
  }
  
  // TODO: check if this is correct
  public void onTimeout(UpdateTimeOut msg) {
    onHeartbeatTimeout(new HeartbeatTimeout());
    setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(msg.epochSeqNum));

    logWithMDC("Timeout for update request: " + msg.epochSeqNum);
  }
  
  public void onReadMessage(ReadDataMsg msg) { /* Value read from Client */
    msg.sender.tell(new DataMsg(getValue()), getSelf());
    logWithMDC("Read read from Client: " + getSender().path().name() + " Value returned: " + currentValue);
  }

  /*
  ************************************* UPDATES HANDLING *************************************
   */

  /* Value update from Client */
  public void onUpdateMessage(WriteDataMsg msg) {
    // Defer messages during election. New messages make part of the next epoch
    if(deferringMessages) {
      deferredMsgSet.add(msg);
      logWithMDC("Deferring message: " + msg.value);
    } else {
      if (isCoordinator()) {
        requestUpdate(msg.value, msg.sender);
      } else {
        logWithMDC("Received update message from client: " + msg.value);
        // forward request to the coordinator
        coordinator.tell(msg, getSelf());

        logWithMDC("Forwarding update message to coordinator: " + coordinator + " with value " + msg.value );
        // Keep message in memory as pending
        pendingMsg.add(msg);
        logWithMDC("Pending message: " + msg.value);
      }
    }
  }

  // CO-HORTS RECEIVE THIS AND SEND ACKS BACK TO THE COORDINATOR
  public void onUpdateRequest(UpdateRequest msg) {
    // Updates must be monotonically increasing within the latest epoch
    expectingAcks = true;
    coordinator.tell(new UpdateAck(msg.value, msg.epochSeqNum), getSelf());
    logWithMDC("Received update request from coordinator. Value: " + msg.value + " SeqNum: " + msg.epochSeqNum);
    
    if(updateTimeOut != null) {
      updateTimeOut.cancel();
    }

    updateTimeOut = setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(msg.epochSeqNum));
    
    // Assume the heartbeat is received
    renewHeartbeatTimeout();
  }

  // COORDINATOR RECEIVES ACKS AND SENDS WRITEOK
  public void onUpdateAck(UpdateAck ack) {
    if (expectingAcks) {
      ActorRef sender = getSender();
      receivedAcks.add(sender);
      if (receivedAcks.size() >= quorum) { // enough acks received, now send WRITEOK message
        logWithMDC("Quorum reached, sending Ok for value " + ack.value);
        ActorRef proposer = pendingRequests.get(ack.epochSeqNum);
        multicast(new WriteOk(ack.value, ack.epochSeqNum, proposer));
        expectingAcks = false; // update phase completed, no more acks expected
      }
    }
  }

  // ALL'S WELL THAT ENDS WELL: COMMIT UPDATE
  public void onWriteOk(WriteOk msg) {
    updateTimeOut.cancel();

    // Check if the message is the next in sequence
    if(msg.epochSeqNum.seqNum == epochSeqNumPair.seqNum + 1){
      logWithMDC("Committing value: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum);

      // store the decision
      commitDecision(msg.value, msg.epochSeqNum);
      
      if(isCoordinator()){
        pendingRequests.remove(msg.epochSeqNum);
      }

      // Check if there are any pending writes that can now be committed
      while(unstableWrites.containsKey(new EpochSeqNum(epochSeqNumPair.currentEpoch, epochSeqNumPair.seqNum + 1))){
        WriteOk writeOk = unstableWrites.get(new EpochSeqNum(epochSeqNumPair.currentEpoch, epochSeqNumPair.seqNum + 1));
        logWithMDC("Committing value: " + writeOk.value + " SeqNum: " + writeOk.epochSeqNum.seqNum);
        commitDecision(writeOk.value, writeOk.epochSeqNum);
        unstableWrites.remove(writeOk.epochSeqNum);
      }
    } else {
      logWithMDC("Defering commit of value: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum + " current epoch seqNum: " + epochSeqNumPair.seqNum);
      // Defer commit until the correct sequence number is reached
      unstableWrites.put(msg.epochSeqNum, msg);
    } 

    // Remove the message from the pending list
    if(msg.proposer.equals(msg.proposer)){
      Iterator<WriteDataMsg> iterator = pendingMsg.iterator();
      while (iterator.hasNext()) {
          WriteDataMsg currentMsg = iterator.next();
          if (currentMsg.value == msg.value) {
              logWithMDC("Removing pending message: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum);
              iterator.remove();
              break; // Exit the loop after removing the first match
          }
      }

      if (isCoordinator()) {
        // Remove the message from the pending requests list
        pendingRequests.remove(msg.epochSeqNum);
      } 

    } 
    // Assume the heartbeat is received
    renewHeartbeatTimeout();
  }

  public void onHeartbeat(Heartbeat msg) {
    if (isCoordinator()) {
      multicast(new Heartbeat());
      
      if(heartbeatTimeout != null) {
        heartbeatTimeout.cancel();
      }
      heartbeatTimeout = setTimeout(HEARTBEAT_INTERVAL, new Heartbeat());

    } else {
      renewHeartbeatTimeout();
    }
  }

  public void onPing(ICMPRequest msg) {
    logWithMDC("Received ping from " + getSender().path().name());
    getSender().tell(new ICMPResponse(), getSelf());
  }

  // USED BY THE COORD TO MULTICAST THE UPDATE REQUEST
  private void requestUpdate(Integer value, ActorRef sender) {
    expectingAcks = true;
    currentRequest++;
    EpochSeqNum esn = new EpochSeqNum(epochSeqNumPair.currentEpoch, currentRequest);
    
    logWithMDC("Requesting update from replicas. Value proposed: " + value + " SeqNum: " + esn);
    multicast(new UpdateRequest(value, esn));

    // Store in memory which replica initiated the request. 
    // The writeOk includes the replica that initiated the request
    // The replica will remove the message from its pending list once the writeOk is received
    pendingRequests.put(esn, sender);
  }
  
  private void renewHeartbeatTimeout(){
    if(heartbeatTimeout != null) {
      heartbeatTimeout.cancel();
    }
    
    heartbeatTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new HeartbeatTimeout());
  }

  /*
  ****************************** ELECTION METHODS AND CLASSES ********************************
   */

  // COORDINATOR CRASHED, THIS NODE IS THE ONE WHICH DETECTED THE CRASH, THUS THE INITIATOR OF THE ELECTION ALGO
  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    logWithMDC("Coordinator is not responding. Starting election.");

    getContext().become(electionMode());
    deferringMessages = true;

    // active replicas are added to the flushes during the election, 
    //these will become the participants in the new epoch
    Map<Integer, Set<ActorRef>> flushes = new HashMap<>();
    Set<ActorRef> activeReplicas = new HashSet<>();
    activeReplicas.add(getSelf());
    flushes.put(epochSeqNumPair.currentEpoch, activeReplicas);

    nextHop = sendElectionMsg(new ElectionMessage(updateHistory, getSelf(), this.id, flushes), nextHop);
    // because of the assumption that there will ALWAYS be a quorum we can safely say that
    // next hop will NEVER be this node
    electionTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new ElectionTimeout(nextHop, flushes));
  }

  public void onElectionAck(ElectionAck ack) {
    electionTimeout.cancel();
    // eventually this will be set to false (assumption that there is always at least a quorum
    nextHopTimedOut = false;
  }

  // if nextHop has crashed, we try with the node after that, until someone sends back an Ack
  public void onElectionTimeout(ElectionTimeout msg) {
    nextHopTimedOut = true;
    // because nextHop timedout we try with the one after that: nextHop+1;
    nextHop = sendElectionMsg(new ElectionMessage(updateHistory,getSelf(),this.id, msg.flushes),msg.next);
    // set timeout with nextHop+1
    electionTimeout = setTimeout(ELECTION_TIMEOUT_DURATION, new ElectionTimeout(nextHop, msg.flushes));
    logger.warn(getSelf().path().name() + " Election timeout. Trying with next node: " + nextHop);
  }

  public void onFlushMessage(FlushMsg msg) {
    if (isCoordinator()) {
      Set<ActorRef> flushedReplicas = this.proposedView.get(this.epochSeqNumPair);
      flushedReplicas.add(getSender());
      if (flushedReplicas.size() >= proposedView.size()) {
        multicast(new ViewChangeMsg(new EpochSeqNum(epochSeqNumPair.currentEpoch++, 0), proposedView.get(epochSeqNumPair), coordinator));
        logWithMDC("View change message sent");
      }
    }
  }

  public void onViewChange(ViewChangeMsg msg) {
    participants.clear();
    
    for(ActorRef participant : msg.proposedView){
      this.participants.add(participant);
      epochSeqNumPair = msg.esn;
      logWithMDC("New participant added: " + participant.path().name());
    }
    coordinator = msg.coordinator;

    logWithMDC( "Participants in the new view: " + participants.toString());
    logWithMDC( "Coordinator in the new view: " + coordinator.path().name());
    getContext().become(createReceive());
    deferringMessages = false;
    
    if (isCoordinator()) {
      multicast(new Heartbeat());
    }

  }


  public void onSyncMessageReceipt(SyncMessage sm){
    this.updateHistory = new HashMap<>(sm.updateHistory);

    // Retry pending messages from current epoch
    for(WriteDataMsg msg : pendingMsg){
      coordinator.tell(new PendingWriteMsg(msg.value, msg.sender), getSelf());
    }
    coordinator.tell(new FlushMsg(), getSelf());
  }

  // on election message receipt, check if up to date
  public void onElectionMessageReceipt(ElectionMessage electionMessage){

    // send ack to whoever sent the message
    if (hasReceivedElectionMessage && !hasBeenUpdated(electionMessage)){
      // END ELECTION
      this.coordinator = proposedCoord;
      cleanUp();
      if (isCoordinator()){
        multicast(new SyncMessage(updateHistory));
        this.proposedView.put(epochSeqNumPair,electionMessage.flushes.get(epochSeqNumPair.currentEpoch));
      }
    } else {
      // Someone else has discovered a crashed coordinator no need to trigger another election
      heartbeatTimeout.cancel();
      deferringMessages = true;
      hasReceivedElectionMessage = true;
      
      getContext().become(electionMode());
      
      getSender().tell(new ElectionAck(), getSelf());
      
      electionMessage.flushes.get(epochSeqNumPair.currentEpoch).add(getSelf());

      // view update content
      proposedCoord = electionMessage.proposedCoordinator;
      proposedCoordinatorID = electionMessage.proposedCoordinatorID;

      List<EpochSeqNum> epochSeqNumList = new LinkedList<>(electionMessage.updateHistory.keySet());
      
      Map<EpochSeqNum, Integer> update = electionMessage.updateHistory;
      // sorting epochs-seqNums in decreasing order;
      epochSeqNumList.sort((o1, o2) -> o1.currentEpoch < o2.currentEpoch ? 1 : -1);
      if (epochSeqNumList.get(0).getCurrentEpoch() > this.epochSeqNumPair.getCurrentEpoch()) {
        // if we're here we missed out 1+ epochs, we must update our history
        int diff = Math.abs(epochSeqNumList.get(0).getCurrentEpoch() - this.epochSeqNumPair.getCurrentEpoch());
        // since epoch updates are sequential we can iterate over the difference diff.
        int pos = 0;
        while (pos < diff) {
          // there's probably a better way to do this but I can't think of it rn :)
          // the idea is that we get the epochSeqNum from the List, then we obtain the value from the hashmap
          // and add those to our updateHistory
          EpochSeqNum key = epochSeqNumList.get(pos);
          Integer value = electionMessage.updateHistory.get(key);
          this.updateHistory.put(key, value);
          pos++;
        }
        // update self, not the message.
        updateCoordinator(electionMessage.proposedCoordinator);
      } else if (epochSeqNumList.get(0).getCurrentEpoch() < this.epochSeqNumPair.getCurrentEpoch()) {
        // we have the latest version so far, we must update the message and forward it.
        proposedCoord = getSelf();
        proposedCoordinatorID = this.id;
        update = this.updateHistory;
      } else {
        // everything matches, break ties using node id
        proposedCoordinatorID = Math.max(proposedCoordinatorID, this.id);
        proposedCoord = proposedCoordinatorID == this.id ? getSelf() : proposedCoord;
      }

      logWithMDC(" Received election message. Proposed coordinator: " + proposedCoordinatorID);
      sendElectionMsg(new ElectionMessage(update, proposedCoord, proposedCoordinatorID, electionMessage.flushes), nextHop);
      electionTimeout = setTimeout(ELECTION_TIMEOUT_DURATION, new ElectionTimeout(nextHop, electionMessage.flushes));
    }
  }
  
  // SOLVE PENDING MESSAGES FROM THE REPLICA BEFORE CHANGING VIEW
  public void onPendingWriteMessage(PendingWriteMsg msg) {
    if (isCoordinator()) {
      requestUpdate(msg.value, msg.sender);
    }
  }

  private void updateCoordinator(ActorRef coordinator){
    this.coordinator = coordinator;
  }

  private boolean hasBeenUpdated(ElectionMessage msg){
    return !msg.updateHistory.equals(this.updateHistory) || msg.proposedCoordinatorID != this.proposedCoordinatorID;
  }

  /**
   *  Logic: sort list of participants, get replica whose id > this.id, then try to send, if timeout update nextHop
   */
  private int sendElectionMsg(ElectionMessage electionMessage, int next){
    if (next == -1) {
      Collections.sort(participants);
      int pos = participants.indexOf(getSelf());
      participants.get((pos + 1) % participants.size()).tell(electionMessage, getSelf());
      return (pos + 1) % participants.size();
    } else if (nextHopTimedOut){
      participants.get((next+1) % participants.size()).tell(electionMessage, getSelf());
      return (next + 1) % participants.size();
    } else {
      participants.get((next) % participants.size()).tell(electionMessage, getSelf());
      return next % participants.size();
    }
  }

  private void cleanUp(){
    this.proposedCoord = null;
    this.proposedCoordinatorID = -1;
  }

  private boolean isCoordinator() {
    if (this.coordinator == null) {
      logger.error("Coordinator is not set");
      return false;
    }
    return coordinator.equals(getSelf());
  }

}
