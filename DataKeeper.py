import zmq
import time
def dataKeeper(NodeIndex,processesIndex,numberOfprocessesOfNodes):
    print("Node =" +str(NodeIndex)+" index = "+ str(processesIndex))
    
    if processesIndex==0:
        port = 5556+NodeIndex
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://127.0.0.1:%s" % str(port))
        start = time.time()
    while True:
        if processesIndex==0:
            if (time.time()-start>=1):         
                topic = 1 #topic ( I am Alive messages)
                messagedata = 1 #alive
                socket.send_string("%d %d %d %d" % (topic, messagedata , NodeIndex,processesIndex))
                start = time.time()
