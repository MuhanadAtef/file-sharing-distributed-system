import zmq

def clientRequestHandler(message, masterDataFile, dataKeepersState, syncLock):
    if message[0] == "upload":
        # Checks whether there is a free port (j) for each ip (i)
        for i in dataKeepersState:
            for j in dataKeepersState[i]:
                syncLock.acquire()
                if(dataKeepersState[i][j]):
                    dataKeepersState[i][j] = False # Make Port Busy
                    syncLock.release()
                    return [i,j,message[1]]
    elif message[0] == "download":
        for i in masterDataFile:
            for j in masterDataFile[i]:
                for k in masterDataFile[i][j]:
                    syncLock.acquire()
                    if k == message[1] and dataKeepersState[i][j]:
                        dataKeepersState[i][j] = False # Make Port Busy
                        syncLock.release()
                        return [i,j,message[1]]
    return None


def masterClientConnection(clientSocket,masterDataFile, dataKeepersState,syncLock):
    # Sending/Receiving data from client
    clientSocket.RCVTIMEO = 1
    # Wait for next request from client
    message = None
    try:
        messege = clientSocket.recv_pyobj()
        print(messege)
    except zmq.error.Again:
        return
    # TODO: Function made by friedPotato7 use messege[upload/download,filename.mp4] and return arr[ip,port#,path of file] replaced by 8000
    # port = ["tcp://localhost:",8000,"Alberto Mardegan - Selfie del futuro.mp4"]
    port = clientRequestHandler(message, masterDataFile, dataKeepersState,syncLock)
    clientSocket.send_pyobj(port)

def masterDatakeeperConnection(masterIndex,datakeeperSocket):
    
    try:
        string = datakeeperSocket.recv_string()
        topic, messagedata , NodeIndex , processesIndex  = string.split()
    except zmq.error.Again:
        return
    if topic=="1" and messagedata=="1" :
        print("Master index "+ str(masterIndex )+" Node " +NodeIndex+" Process "+ processesIndex +" is Alive\n")



def initialzeClientMasterConnection(masterIndex,startingPortMasterClient):
    # Bind ports for clients
    clientPort=startingPortMasterClient+masterIndex
    context = zmq.Context()
    clientSocket = context.socket(zmq.REP)
    clientSocket.bind("tcp://*:%s" % clientPort)
    return clientSocket



def initialzeDatakeeperMasterConnection(masterIndex,numberOfNodes_Datakeeper):
    # Bind ports for datakeeper
    print("Master index = "+ str(masterIndex))

    context = zmq.Context()
    datakeeperSocket = context.socket(zmq.SUB)
    datakeeper_StartPort = 5556
    portArr=[]
    for i in range(numberOfNodes_Datakeeper):     
        t=datakeeper_StartPort+i
        portArr.append(t)
    for j in portArr:
        datakeeperSocket.connect ("tcp://127.0.0.1:%s" %  str(j))
    topicfilter = "1"
    datakeeperSocket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
    datakeeperSocket.RCVTIMEO = 1
    return datakeeperSocket


def masterTracker(masterIndex,numberOfNodes_Datakeeper,startingPortMasterClient,masterDataFile,dataKeepersState,syncLock, filesDictionary):
    
    clientSocket = initialzeClientMasterConnection(masterIndex,startingPortMasterClient)
    datakeeperSocket = initialzeDatakeeperMasterConnection(masterIndex,numberOfNodes_Datakeeper)
    
    while True:
        #Connecting with client
        masterClientConnection(clientSocket,masterDataFile, dataKeepersState, syncLock)
        
        # Connecting with data keepers
        masterDatakeeperConnection(masterIndex,datakeeperSocket)
        
        