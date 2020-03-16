import zmq
import random

def client(masterIp,startingPortMasterClient,numberOfprocessesOfMaster,commands):
    context = zmq.Context() 
    masterSocket = context.socket(zmq.REQ) # Master-client connection
    datakeeperSocket = context.socket(zmq.PAIR) # Datakeeper-client connection
    ls=[] # Holding ports of the master to be connected to client
    # Generate random connection to master processes
    for i in range(numberOfprocessesOfMaster):
        ls.append(startingPortMasterClient+i)
    ran=random.sample(ls,k=numberOfprocessesOfMaster)
    for i in ran:
        masterSocket.connect(masterIp + str(i))
    
    print("---------------------------------------------------------------")
    print("-- Client connected to all master processes successfully !!! --")
    print("---------------------------------------------------------------")

    for i in commands:
        # Getting path and command
        com = i.split()
        command=com[0]
        path=""
        for j in range(len(com)):
            if j==0:
                command=com[j]
            else:
                path+=com[j]
                if j != len(com)-1:
                    path+=" "

        if command=="upload" or command=="download":
            messege = None
            while (messege == None):
                masterSocket.send_pyobj([command,path])
                messege = masterSocket.recv_pyobj()
            datakeeperSocket.connect(str(messege[0])+str(messege[1]))
            print (command+"ing ...")
            if command=="upload":
                f= open(path,'rb')
                video=f.read()
                datakeeperSocket.send_pyobj([video,messege[2]])
                f.close()
                datakeeperSocket.recv()
                datakeeperSocket.close()
                print("File uploaded successfully")
            else:
                datakeeperSocket.send_pyobj([path])
                video=datakeeperSocket.recv_pyobj()
                name=video[1].split("/")
                f=open(name[-1],'wb')
                f.write(video[0])
                f.close()
                print("Download completed successfully")
                datakeeperSocket.close()
        else:
            print("Unknown command")
