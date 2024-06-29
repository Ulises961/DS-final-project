* Replica sends updateAck -> coordinator crash (no WriteOk)
    * replica timeout -> proposed value remains pending
    * enter election mode
    * defer update messages
    * choose a new coordinator
    * conclude current update

* Replica sends an update message -> coordinator never answers UpdateRequest 
    * replica timeout
    * enter election mode
    * defer update messages
    * when new coordinator arrives repropose value before entering into the new Epoch

* Coordinator sends SyncMessage 
  * onSyncMessage Replicas send pending updates 
  * once received confirmations for pending messages send flush message
  * when coordinator has all flushes sends change view
  * on change view replica removes pending updates

* If new Coordinator crashes, repeat flow using pending updates

* In concurrent updateRequests
coordinator keeps a queue of pending updateRequests
    * consumes them one by one. 
    * Replica only removes from its pending queue whenever accepted
    * If coordinator crashes the replica will retry update with next coordinator


* When going round the ring count how many live nodes are there, 
  *  the live nodes will be the ones in the next view
the size of the ring will count to check the number of flushes necessary to change view


* If there is a crash during the new view installation a new election with a new view must be installed

