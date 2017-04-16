/** We don't use a neighbor list to keep track of every node's neighbor in this program.We rely instead on a naming scheme that assigns each
each node the value of its coordinates.These coordinates are then used to locate neighbors since we know the type of topology that will be used
beforehand, thus saving memory and eliminating the need for each node to have a lengthy list of nodes especially in the case of Full topology.*/

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.io.File
import akka.actor._
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext
import akka.actor.Actor
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import akka.actor.Props
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import akka.actor.ActorSelection
import Math._
//A few global variable to keep track of the total number of nodes that have been parsed,index values etc.
object Globs {
    def counter = 10
    var nodes_parsed:Int = 0
    var svalcount:Double=0
}
//Definition of the case objects
case object BuildTopo
case class InitiateGossip(gossip_message:String)
case class Gossip(msg:String,s:Double,w:Double)
case class AssignCords(x:Integer,y:Integer,z:Integer)
case object EndGossip


//The node class represents each of the nodes that will form a part of the topology
class Node(no_of_nodes:Integer,topology:String,algorithm:String) extends Actor {
    import context._
    import Globs._
    var ratio = ArrayBuffer[Boolean](false,false,false)
    var prev_ratio=0.0
    var new_ratio=0.0
    var ct:Int=0
    val oneDList = List(-1, 1)
    val threeDlist:ArrayBuffer[ArrayBuffer[Int]]= ArrayBuffer(
        ArrayBuffer(0,1,0),
        ArrayBuffer(0,-1,0),
        ArrayBuffer(-1,0,0),
        ArrayBuffer(1,0,0),
        ArrayBuffer(0,0,1),
        ArrayBuffer(0,0,-1)
    )
    var random_neighbor:ActorSelection = _
    var neighbor_list=new ListBuffer[ActorRef]()
    var msg_counter:Integer = 0
    var xcord:Integer = 0
    var ycord:Integer = 0
    var zcord:Integer = 0
    var s:Double = 0.0
    var w:Double = 1.0
    def receive = {
        /**Assigncords is used assign the coordinates to the node based on the different topologies.Imp3D takes a little extra effort
        since it requires its random neighbor to be computed as well.*/
        case AssignCords(x:Integer,y:Integer,z:Integer) => {
            xcord = x
            ycord = y
            zcord = z
            s=x*100.0+y*10.0+z*1.0
            svalcount+=s
            if(topology=="imp3D") {
                var dims:Double=cbrt(no_of_nodes.toDouble)
                var dimen = dims.toInt 
                var rand_dim:Integer=(dimen/2).toInt
                var xnei:Integer=(x+(rand_dim))%(dimen)
                var ynei:Integer=(y+(rand_dim))%(dimen)
                var znei:Integer=(z+(rand_dim))%(dimen)
                threeDlist+=ArrayBuffer(xnei,ynei,znei)
                println("Neighbour Random for:(" +xcord+","+ycord+","+zcord+")"+"="+xnei+ynei+znei)
            }
            
        }
        //Matches the algorithm and calls the corresponding function
        case Gossip(msg:String,s_in:Double,w_in:Double) => {
            algorithm match {
                case "gossip" => gossip_algo(msg)
                case "push-sum" => push_sum(msg,s_in,w_in)
            }
        }
    }
    //Implements the gossip algorithm
    def gossip_algo(msg:String) = {
        if (msg_counter <= counter) {
                random_neighbor = getRandomNeighbor()
                random_neighbor ! Gossip(msg,s,w)
                msg_counter = msg_counter + 1
                if(msg_counter==1){
                nodes_parsed = nodes_parsed + 1
            }
        } else {
            println("The total number of nodes that heard this message were:"+nodes_parsed)
            var master = context.actorSelection("/user/master")
            master ! EndGossip
        }       
    }
    //Implements the push-sum algorithm
    def push_sum(msg:String,s_in:Double,w_in:Double) {
        prev_ratio = s/w
        s = s + s_in
        w = w + w_in
        new_ratio = s/w
        println("The ratio at the node:("+xcord+","+ycord+","+zcord+")"+"is:"+new_ratio)

        if (Math.abs(new_ratio - prev_ratio) < 0.00000000001) {
            ratio(ct) = true
            ct = ct + 1
        }

        if (ratio(0) && ratio(1) && ratio(2)) {
            if(topology=="3D"||topology=="imp3D"){
            var dims:Double=cbrt(no_of_nodes.toDouble)
            var dimen = dims.toInt
            var deltae:Double=Math.abs(new_ratio-prev_ratio)
            dimen=dimen*dimen*dimen 
            println("The theoretical s/w ratio calculated was:"+(svalcount/(dimen)))
            }
            else{
                println("The theoretical s/w ratio calculated was:"+(svalcount/(no_of_nodes)))   
            }
            var deltae:Double=Math.abs(new_ratio-prev_ratio)

            println("The estimated ratio was:"+new_ratio)
            var master = context.actorSelection("/user/master")
            master ! EndGossip
        } else {
            random_neighbor = getRandomNeighbor()
            random_neighbor ! Gossip(msg,s/2,w/2)
            s = s/2
            w = w/2
        }
    }
    //This is the function used to compute and return a random neighbor for the different topologies.
    def getRandomNeighbor() : ActorSelection = {
        val rowList = ArrayBuffer(0, 1, 2, 3, 4, 5)
        var x_index = 0
        var y_index = 0
        var z_index = 0
        var randomval = new Random()

        topology match {
            case "line"  => {
                var n_index = xcord + oneDList(randomval.nextInt(oneDList.size))
                if ( n_index < 0) {
                    n_index = n_index + 2
                }
                else if (n_index > (no_of_nodes-1)) {
                    n_index = n_index - 2
                }
                random_neighbor = context.actorSelection("/user/node"+n_index)
            }

            case "3D" => {
                var dim:Double=cbrt(no_of_nodes.toDouble)
                dim = dim.toInt
                dim=dim-1
                while (!(rowList.isEmpty)) {
                    var list_index = randomval.nextInt(rowList.length)
                    var row = rowList(list_index)
                    x_index = xcord + threeDlist(row)(0)
                    y_index = ycord + threeDlist(row)(1)
                    z_index = zcord + threeDlist(row)(2)
                    
                    if (x_index < 0 || y_index < 0 || z_index < 0 || x_index > dim || y_index > dim || z_index > dim) {
                        rowList.remove(list_index)
                    } else {
                        var nei:String = "%04d".format(x_index)+"%04d".format(y_index)+"%04d".format(z_index)
                        random_neighbor = context.actorSelection("/user/node"+nei)
                        return random_neighbor
                    }
                }
            }

            case "imp3D" => {
                var dims:Double=cbrt(no_of_nodes.toDouble)
                var dim = dims.toInt
                dim=dim-1
                rowList+=6
                while (!(rowList.isEmpty)) {
                    var list_index = randomval.nextInt(rowList.length)
                    if(list_index==rowList.length-1){
                        var row = rowList(list_index)
                        x_index = threeDlist(row)(0)
                        y_index = threeDlist(row)(1)
                        z_index = threeDlist(row)(2)
                        var nei:String = "%04d".format(x_index)+"%04d".format(y_index)+"%04d".format(z_index)
                        random_neighbor = context.actorSelection("/user/node"+nei)
                        return random_neighbor

                    }
                    var row = rowList(list_index)
                    x_index = xcord + threeDlist(row)(0)
                    y_index = ycord + threeDlist(row)(1)
                    z_index = zcord + threeDlist(row)(2)
                    
                    if (x_index < 0 || y_index < 0 || z_index < 0 || x_index > dim || y_index > dim || z_index > dim) {
                        rowList.remove(list_index)
                    } else {
                        println("Neighbour Dimensions for:(" +xcord+","+ycord+","+zcord+")"+"="+x_index+y_index+z_index)
                        var nei:String = "%04d".format(x_index)+"%04d".format(y_index)+"%04d".format(z_index)
                        random_neighbor = context.actorSelection("/user/node"+nei)
                        return random_neighbor
                    }
                }
            }
            
            case "full"  => {
                var n_index = randomval.nextInt(no_of_nodes)
                random_neighbor = context.actorSelection("/user/node"+n_index)
                println("The coordinates of the random neighbor for("+xcord+"):"+n_index)
            }
        }
        return random_neighbor   
    }
}
//Master actor initiates the gossip and shuts the system down when the termination condition is reached.
class Master(no_of_nodes:Integer,topology:String,algorithm:String) extends Actor {

    val b = System.currentTimeMillis
    var seedList= ArrayBuffer(0,0,0)
    var x_index=0
    var y_index=0
    var z_index=0

    var fn: ActorSelection = _
    def receive = {
        //The different topologies used are built here.    
        case BuildTopo => {
            
            topology match {
                
                case "line" => {
                    println("inside build topo 1d")
                    build_1d()
                    fn = context.actorSelection("/user/node"+seedList(0))
                }
                case "3D" => {
                    println("inside build topo 3d")
                    build_3d()
                    var str_index="%04d".format(seedList(0))+"%04d".format(seedList(1))+"%04d".format(seedList(2))
                    fn = context.actorSelection("/user/node"+str_index) 
                }
                case "imp3D" => {
                    println("inside build topo imp3D")
                    build_3d()
                    var str_index="%04d".format(seedList(0))+"%04d".format(seedList(1))+"%04d".format(seedList(2))
                    fn = context.actorSelection("/user/node"+str_index) 
                }
                case "full" => {
                    build_1d()
                    fn = context.actorSelection("/user/node"+seedList(0))
                }
            } 
        }

        case InitiateGossip(gossip_message:String) => {
            println("Gossip Initiated.")
            fn ! Gossip(gossip_message,0,0)
        }

        case EndGossip => {

        if(algorithm =="gossip"){
            println("One of the actors has heard the gossip ten times.Shutting the system down.")
        }
        else{
            println("The push-sum algorithm has ended, Shutting down the system.")
        }
            println("Time taken in milliseconds: " + (System.currentTimeMillis - b))
            context.system.shutdown()
        }
    }

    def build_1d() {
        for(x <- 0 to (no_of_nodes-1)) {
            var node = context.system.actorOf(Props(new Node(no_of_nodes,topology,algorithm)), name = "node"+x)
            node ! AssignCords(x,0,0)
            var randomval = new Random()
            x_index = randomval.nextInt(no_of_nodes)
            y_index = 0
            z_index = 0
            seedList = ArrayBuffer(x_index,y_index, z_index)
        }
        println("Topology build completed.")
    }

    def build_3d() {
        println("3D topology build initiated.")
        var cube_dims:Double=cbrt(no_of_nodes.toDouble)
        var cube_dim:Integer = cube_dims.toInt
        println("Cube dimensions:"+ cube_dim)
        for(z<-0 until cube_dim) {
            for(y <-0 until cube_dim){
                for( x<-0 until cube_dim){
                    var str_index:String ="%04d".format(x)+"%04d".format(y)+"%04d".format(z)
                    var node = context.system.actorOf(Props(new Node(no_of_nodes,topology,algorithm)), name = "node"+str_index)
                    node ! AssignCords(x,y,z)
                }
            }
        }
        println("3D topology build completed.")
        var randomval = new Random()
        x_index = randomval.nextInt(cube_dim)
        y_index = randomval.nextInt(cube_dim)
        z_index = randomval.nextInt(cube_dim)
        seedList = ArrayBuffer(x_index,y_index, z_index)
    }
}

object GossipSystem extends App {

    var no_of_nodes = args(0).toInt
    var topology: String=args(1)
    var algorithm: String=args(2)
    val system = ActorSystem("GossipSystem")
    val master = system.actorOf(Props(new Master(no_of_nodes,topology,algorithm)), name = "master")
    master ! BuildTopo
    master ! InitiateGossip("This is a rumour")

}
