# README #

This is an sbt-based project in scala which uses Akka's Actor framework.Run the sbt from the command line while specifying the number of nodes, the network topology you wish to use and the type of connection you want the nodes to have with said topology.

### About project ###

This is a project that implements the gossip protocol described in this paper (http://www.cs.cornell.edu/johannes/papers/2003/focs2003-gossip.pdf), using actors in Akka. Different network topologies were tested for determining the rate of dissemination of information and other results and interesting observations were included in a report.

The build file specifies any scala and akka related dependencies you may need. You will need to have installed Scala (http://www.scala-lang.org/download/) and sbt(http://www.scala-sbt.org/download.html).