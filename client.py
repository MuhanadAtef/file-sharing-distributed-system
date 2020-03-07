import zmq
import random

def clientMasterConnection(master_ip,starting_port,master_ports,commands):
    context = zmq.Context()
    masterSocket = context.socket(zmq.REQ)
    datakeeperSocket = context.socket(zmq.PAIR)
    ls=[] # Holding ports of the master to be connected to client
    # Generate random connection to master processes
    for i in range(master_ports):
        ls.append(starting_port+i)
    ran=random.sample(ls,k=master_ports)
    for i in ran:
        masterSocket.connect(master_ip + str(i))
    
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
            masterSocket.send_pyobj([command,path])
            messege = masterSocket.recv_pyobj()
            datakeeperSocket.connect(str(messege[0])+str(messege[1]))
            print (messege)
            if command=="upload":
                f= open(path,'rb')
                video=f.read()
                datakeeperSocket.send_pyobj([video,path])
                f.close()
                datakeeperSocket.recv()
                datakeeperSocket.close()
            else:
                video=datakeeperSocket.recv_pyobj()
                f=open(path,'wb')
                f.write(video)
                datakeeperSocket.close()
        else:
            print("Unknown command")
        
def client(master_ip,starting_port,master_ports,commands):
    clientMasterConnection(master_ip,starting_port,master_ports,commands)
        



