import multiprocessing
import Client


if __name__ == '__main__':
    
    ##############################################################################################################################################
    ################# PLEASE EITHER HARDCODE THE FOLLOWING IPS AND COMMENT THE FOLLOWING INPUT BLOCK OR INPUT THEM THROUGH CLI ###################
    ##############################################################################################################################################
    
    masterIP = "172.30.249.130"  # master ip
    
    ################################################################################
    ################################## INPUT BLOCK #################################
    ################################################################################
    
    print("/////////////////////////////////////////////////////////////////////\n")
    print("//////////////// Please enter the master server IP //////////////////\n")
    print("/////////////////////////////////////////////////////////////////////\n")
    input(masterIP)
    
    ##############################################################################################################################################
    
    # number of processes  multi-process(MasterTracker)
    numberOfprocessesOfMaster = 5
    startingPortMasterClient = 7000  # first port between client/master
    masterIP = "tcp://" + masterIP + ":"
    processes=[]
    
    print("Enter # of client processes:")
    numberOfClients = input()
    for i in range(int(numberOfClients)):
        commands = []
        print("Enter # of commands for process #%d" % i)
        n = input()
        for j in range(int(n)):
            print("command #%d:" % j)
            x = input()
            commands.append(x)
        t = multiprocessing.Process(target=Client.client, args=(
            masterIP, startingPortMasterClient, numberOfprocessesOfMaster, commands))
        processes.append(t)

    for j in processes:
        j.start()

    for j in processes:
        j.join()
