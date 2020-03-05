import zmq

def masterClientConnection(masterIndex):
    # Bind ports for clients
    clientPort=7000+masterIndex
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % clientPort)
    # Sending/Receiving data from client
    while True:
        # Wait for next request from client
        messege = socket.recv_pyobj()
        print(messege)
        # TODO: Function made by friedPotato7 use messege[upload/download,filename] and return arr[ip,port#,path of file] replaced by 8000
        port=["tcp://*:",8000,"/file"]
        socket.send_pyobj(port)
        print(port)
        if messege[0]=="upload":
            socket.recv_string()
            socket.send_string("success")
            

def masterDatakeeperConnection(masterIndex,numberOfNodes_Datakeeper):
    print("Master index = "+ str(masterIndex))
    if masterIndex==0: 
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        datakeeper_StartPort = 5556
        portArr=[]
        for i in range(numberOfNodes_Datakeeper):     
            t=datakeeper_StartPort+i
            portArr.append(t)
        for j in portArr:
            socket.connect ("tcp://127.0.0.1:%s" %  str(j))
        topicfilter = "1"
        socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)

    while True:
        if masterIndex==0:
            string = socket.recv_string()
            topic, messagedata , NodeIndex , processesIndex  = string.split()
            if topic=="1" and messagedata=="1" :
                print("Master index "+ str(masterIndex )+" Node " +NodeIndex+" Process "+ processesIndex +" is Alive\n")


def masterTracker(masterIndex,numberOfNodes_Datakeeper):
    
    # TODO: make 2 threads for the 2 functions to run in parallel

    #Connecting with client
    masterClientConnection(masterIndex)

    # Connecting with data keepers
    masterDatakeeperConnection(masterIndex,numberOfNodes_Datakeeper)