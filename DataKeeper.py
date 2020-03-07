import zmq
import time
def dataKeeper(NodeIndex,processesIndex,startingPortDatakeeperClient):
    print("Node =" +str(NodeIndex)+" index = "+ str(processesIndex))
    context = zmq.Context()
    if processesIndex==0:
        port = 5556+NodeIndex
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://127.0.0.1:%s" % str(port))
        start = time.time()
    # Bind ports of datakeeper to be used with client
    context2 = zmq.Context()
    clientSocket=context2.socket(zmq.PAIR)
    clientSocket.bind("tcp://*:"+str(int(startingPortDatakeeperClient+processesIndex)))
    clientSocket.RCVTIMEO = 1

    while True:
        if processesIndex==0:
            if (time.time()-start>=1):         
                topic = 1 #topic ( I am Alive messages)
                messagedata = 1 #alive
                socket.send_string("%d %d %d %d" % (topic, messagedata , NodeIndex,processesIndex))
                start = time.time()

        # Connection with client
        data=[]
        try:
            data=clientSocket.recv_pyobj()
        except zmq.error.Again:
            continue

        if len(data)==2:    # Client upload
            name=data[1].split("/") # To download in the same location of the file
            f=open(name[-1],'wb')
            f.write(data[0])
            f.close()
            print("File uploaded successfully")
            # TODO: Friedpotato7 || Mahmoud send success msg to master
            clientSocket.send_string("")

        elif len(data)==1:  # Client download
            f= open(data[0],'rb')
            video=f.read()
            clientSocket.send_pyobj([video,data[0]])
            f.close()