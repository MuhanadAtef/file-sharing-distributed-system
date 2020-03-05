import zmq

def client(master_ip,master_ports):
    context = zmq.Context()
    for i in range(master_ports):
        socket = context.socket(zmq.REQ)
        socket.connect(master_ip + str(i+7000))
    print("Client connected to all master processes successfully!!!")

    i=0
    while True:
        messege=["",0]
        if i==10:
            break
        if i%2==0:
            print ("Download %d...\n" %i)
            messege=["download",i]
        else:
            messege=["upload",i]
        socket.send_pyobj(messege)
        #  Get the reply.
        messege = socket.recv_pyobj()
        print ("receive 1\n")
        if i%2 != 0:
            # Sending dummy data as we want one more receive from master "success messege"
            socket.send_string("")
            #  Get the reply.
            messege = socket.recv_string()
            if messege=="success":
                print("upload completed successfully")
        i+=1
        
        



