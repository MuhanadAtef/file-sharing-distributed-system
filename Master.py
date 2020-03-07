import zmq

def masterClientConnection(clientSocket):
    # Sending/Receiving data from client
    clientSocket.RCVTIMEO = 1
    # Wait for next request from client
    try:
        messege = clientSocket.recv_pyobj()
        print(messege)
    except zmq.error.Again:
        return
    # TODO: Function made by friedPotato7 use messege[upload/download,filename.mp4] and return arr[ip,port#,path of file] replaced by 8000
    port=["tcp://localhost:",8000,"Alberto Mardegan - Selfie del futuro.mp4"]
    clientSocket.send_pyobj(port)

def masterDatakeeperConnection(masterIndex,datakeeperSocket):
    if masterIndex==0:
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
    if masterIndex==0: 
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


def masterTracker(masterIndex,numberOfNodes_Datakeeper,startingPortMasterClient):
    
    clientSocket = initialzeClientMasterConnection(masterIndex,startingPortMasterClient)
    datakeeperSocket = initialzeDatakeeperMasterConnection(masterIndex,numberOfNodes_Datakeeper)
    
    while True:
        #Connecting with client
        masterClientConnection(clientSocket)
        if masterIndex==0: 
            # Connecting with data keepers
            masterDatakeeperConnection(masterIndex,datakeeperSocket)
        
        