# Quorum-based total order broadcast
This project implements a protocol for the coordination of a group of replicas sharing the same mock data.
A group of clients sends read and writes requests and all replicas agree by means of a two-phase broadcast. The cluster of replicas is coordinated
by a specific replica that determines the order of writes.
The protocol implements a ring based election in the event of a crashed coordinator, tolerates multiple replica crashes. 
Furthermore, the decision of the value is decided using a quorum based approach.

## Characteristics
### Total order
* Replicas can only commit a value in the order specified by the coordinator.
### Virtual Synchrony - Correct epoch delivery
* Messages for which the decision was interrupted due to a coordinator crash are delivered in the current epoch. Instead, messages received during the election of
the new coordinator are placed in the subsequent epoch. 
### Correctness and Liveness 
* The commitment of new writes is decided by a quorum of replicas. This ensures the protocol continues even on slow links.
* The election of a new coordinator is ensured to finish using the nodes id as a tie breaker.
* Failed elections restarts if not decided after a fixed amount of time
* Crashed replicas are discovered using time outs.

## How to run
Clone the repository, navigate to the root folder of the project and run:
``` bash
gradle run
```
A console based menu displays the options to choose from and test the correctness algorithm.
