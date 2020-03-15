import zmq
import time
import socket

def getIp():
    s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    s.connect(("8.8.8.8",80))
    return s.getsockname()[0]


def dataKeeper(NodeIndex,processesIndex,startingPortDatakeeperClient,masterCount,masterIp):
    print("Node =" +str(NodeIndex)+" index = "+ str(processesIndex))

    context = zmq.Context()
    ipSender = context.socket(zmq.PUSH)
    ipSender.connect(masterIp + "17777")
    address = {"ip": getIp(), "port": startingPortDatakeeperClient + processesIndex}
    ipSender.send_pyobj(address)
    if processesIndex==0:
        port = 5556+NodeIndex
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://127.0.0.1:"+"%s" % str(port))
        start = time.time()
    # Bind ports of datakeeper to be used with client
    context2 = zmq.Context()
    clientSocket=context2.socket(zmq.PAIR)
    clientSocket.bind("tcp://*:"+str(int(startingPortDatakeeperClient+processesIndex)))
    clientSocket.RCVTIMEO = 1
    # connect ports of datakeeper to be used with Master
    context3 = zmq.Context()
    masterSocket = context3.socket(zmq.SUB)
    topicfilter = "1"

    for i in range(masterCount):
        port =10000+i
        masterSocket.connect ("tcp://localhost:%s" % port) #hena el mafrood no7ot el ip bta3 el master
    #masterSocket.setsockopt(zmq.SUBSCRIBE, topicfilter)
    print("----------------------------------------------------------------------------------")
    print("-- Datakeeper connected to all master processes successfully (n-replicates) !!! --")
    print("----------------------------------------------------------------------------------")

    

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

        #connection with master
        data2=[]
        try:
            data2=clientSocket.recv_pyobj()        
        except zmq.error.Again:
            continue

        # Nreplicates connection with master
        data3=[]
        try:
            data3 =masterSocket.recv() 
            topic, messagedata = data3.split()       
        except zmq.error.Again:
            continue
        
        if topic=="1" and len(messagedata)==3: #message from Master to sourceMachine dataKeeper here source machine datakeeper send the video to another data keeper so at machine_to_copy it will get in "client upload" as if a client send this file to it
            if messagedata[2]=="source_machine":
                contextt = zmq.Context()
                datakeeperSocket = contextt.socket(zmq.PAIR) # Datakeeper-Datakeeper connection
                datakeeperSocket.connect(messagedata[0])
                f= open(messagedata[1],'rb')
                video=f.read()
                datakeeperSocket.send_pyobj([video,messagedata[1]])
                f.close()
                datakeeperSocket.recv()
                datakeeperSocket.close()


        if len(data)==2:    # Client upload
            name=data[1].split("/") # To download in the same location of the file
            f=open(name[-1],'wb')
            f.write(data[0])
            f.close()
            print("File uploaded successfully")
            # send to master that it is successfully uploaded
            #---------------------------------------------------
            topic=2
            messagedata = name
            ip = getIp()
            port=str(int(startingPortDatakeeperClient+processesIndex))
            socket.send_string("%d %d %d %d" % (topic, messagedata ,ip,port))
            #-------------------------------------------------------------------
            clientSocket.send_string("")

        elif len(data)==1:  # Client download
            f= open(data[0],'rb')
            video=f.read()
            clientSocket.send_pyobj([video,data[0]])
            f.close()

