/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */


/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    // Create entry for self in memberList
    MemberListEntry* memberEntry = new MemberListEntry(id,port,memberNode->heartbeat,par->getcurrtime());
    memberNode->memberList.push_back(*memberEntry);
    #ifdef DEBUGLOG
            log->logNodeAdd(&memberNode->addr,&memberNode->addr);
    #endif
    delete memberEntry;

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   memberNode->inited = false;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr msg, *returnMsg;
    Address senderAddr, newNodeAddress;
    long senderHeartbit;
    vector<MemberListEntry> entryList, updateList;
    vector<MemberListEntry>::iterator iter;
    unsigned entryListSize;

    Address joinaddr = getJoinAddress();
    // This node Id and Port
    int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);
    
    // Retrieve message type and Sender address
    memcpy(&msg,data,sizeof(MessageHdr));
    memcpy(&senderAddr.addr,&data[sizeof(MessageHdr)] ,sizeof(senderAddr.addr));

    switch(msg.msgType){
        case JOINREQ:
            // confirm I am the Introducer - Then Handle JOINREQ message
            if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr.addr), sizeof(memberNode->addr.addr))) {
                memcpy(&senderHeartbit,&data[sizeof(MessageHdr)+1+ sizeof(senderAddr.addr)],sizeof(long));
  
                // create JOINREP message: format of data is {struct Address myaddr}
                size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(MemberListEntry)*(memberNode->memberList.size()) + 1;
                returnMsg = (MessageHdr *) malloc(msgsize * sizeof(char));
                unsigned membershipListSize = memberNode->memberList.size();
                returnMsg->msgType = JOINREP;
                memcpy((char *)(returnMsg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
                memcpy((char *)(returnMsg+1) + 1 + sizeof(memberNode->addr.addr),&membershipListSize,sizeof(unsigned));
                memcpy((char *)(returnMsg+1) + 1 + sizeof(memberNode->addr.addr) +sizeof(unsigned), &memberNode->memberList[0], 
                        sizeof(MemberListEntry)*memberNode->memberList.size());
                // send JOINREP message to originator node
                emulNet->ENsend(&memberNode->addr, &senderAddr, (char *)returnMsg, msgsize);
                free(returnMsg);

                // Add new member to membership list of coordinator
                int senderId = *(int*)(&senderAddr.addr);
	            int senderPort = *(short*)(&senderAddr.addr[4]);
                MemberListEntry* memberEntry = new MemberListEntry(senderId,senderPort,senderHeartbit,par->getcurrtime());
                memberNode->memberList.push_back(*memberEntry);
                #ifdef DEBUGLOG
                log->logNodeAdd(&memberNode->addr,&senderAddr);
                #endif
                delete memberEntry;
                
            }
            break;

        case JOINREP:
            memcpy(&entryListSize,&data[sizeof(MessageHdr)+1+ sizeof(senderAddr.addr)],sizeof(unsigned));
            entryList.resize(entryListSize);
            memcpy(&entryList[0],&data[sizeof(MessageHdr)+1+ sizeof(senderAddr.addr)+sizeof(unsigned)],
                sizeof(MemberListEntry)*entryListSize);
            // Update timestamp of received list to local
            cout<<"JOINREP at "<<memberNode->addr.getAddress()<<":: \t\t";
            for(iter = entryList.begin(); iter != entryList.end(); iter++) {
                cout<<iter->id<<" ";
                iter->settimestamp(par->getcurrtime());
            }
            cout<<endl;
            // Append received entry list to receiving node list
            memberNode->memberList.insert(memberNode->memberList.end(),entryList.begin(), entryList.end());
            #ifdef DEBUGLOG
                for(iter = entryList.begin(); iter != entryList.end(); iter++) {
                    newNodeAddress = getAddress(iter->id, iter->port);
                    log->logNodeAdd(&memberNode->addr,&newNodeAddress);
                }
            #endif
            // Officially join the group
            memberNode->inGroup = true;
            break;

        case HEARTBEAT:
            if(memberNode->inGroup){
                memcpy(&entryListSize,&data[sizeof(MessageHdr)+1+ sizeof(senderAddr.addr)],sizeof(unsigned));
                entryList.resize(entryListSize);
                memcpy(&entryList[0],&data[sizeof(MessageHdr)+1+ sizeof(senderAddr.addr)+sizeof(unsigned)],
                    sizeof(MemberListEntry)*entryListSize);

                // Scan received membership list and merge with own
                int senderId = *(int*)(&senderAddr.addr);
                bool nodeFound;
                updateList.clear();
                for(iter = entryList.begin(); iter != entryList.end(); iter++) {
                    if(iter->id != id){
                        nodeFound = false;
                        for(memberNode->myPos = memberNode->memberList.begin(); 
                        memberNode->myPos != memberNode->memberList.end(); memberNode->myPos++) {
                            if(iter->id == memberNode->myPos->id) {
                                nodeFound = true;
                                // check that node is not failed
                                if((par->getcurrtime() - memberNode->myPos->timestamp) < TFAIL) {
                                    // update heartbeat
                                    if(iter->heartbeat > memberNode->myPos->heartbeat) {
                                        memberNode->myPos->setheartbeat(iter->heartbeat);
                                        memberNode->myPos->settimestamp(par->getcurrtime());
                                        //cout<<id<<" updated heartbeat for node "<<iter->id<<" to "<<memberNode->myPos->heartbeat<<"\n";
                                    }
                                }


                            }
                        }
                        if(!nodeFound){
                            // put in temporary list
                            MemberListEntry* someEntry = new MemberListEntry(iter->id,iter->port,
                                            iter->heartbeat,par->getcurrtime());
                            updateList.push_back(*someEntry);
                            //cout<<iter->id<<" added to membership list of "<<id<<"\n";
                            delete someEntry;

                        }

                        
                    }

                }
                // Add new members in the membership list
                if(updateList.size() > 0)  {
                    memberNode->memberList.insert(memberNode->memberList.end(),updateList.begin(), updateList.end());
                    #ifdef DEBUGLOG
                    cout<<"HeartBeating Update List at "<<memberNode->addr.getAddress()<<":: \t\t\t";
                    for(iter = updateList.begin(); iter != updateList.end(); iter++) {
                        cout<<iter->id<<" ";
                        newNodeAddress = getAddress(iter->id, iter->port);
                        log->logNodeAdd(&memberNode->addr,&newNodeAddress);
                    }
                    cout<<endl;
                    #endif
                }
            }


            
    }
    
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    MessageHdr *msg;
    Address targetAddress;
    unsigned btarget = 6;
    int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

    // Update heartbeat and remove TREMOVE failed nodes from membership list
    memberNode->heartbeat++;
    vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
    while(iter != memberNode->memberList.end()){
        if((iter->id == id) && (iter->port == port) ) {
            iter->setheartbeat(memberNode->heartbeat);
            iter->settimestamp(par->getcurrtime());
        }
        else if((par->getcurrtime() - iter->timestamp) > TREMOVE) {
            Address rmvAdress = getAddress(iter->id, iter->port);
            iter = memberNode->memberList.erase(iter);
            #ifdef DEBUGLOG
                log->logNodeRemove(&memberNode->addr,&rmvAdress);
            #endif 
            continue;    
        }
        iter++;
    }
    // Create entry list for Heartbeating - remove TFAIL entries
    vector<MemberListEntry> entryList = memberNode->memberList;
    iter = entryList.begin();
    cout<<"Current membership at node "<<memberNode->addr.getAddress()<<">>\t";
    while(iter != entryList.end()){
        cout<<iter->id<<" ";
        if((iter->id != id) && ( (par->getcurrtime() - iter->timestamp) > TFAIL ) ) {
            iter = entryList.erase(iter);
        }
        else
            iter++;
    }
    cout<<endl;

    //Create Message
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(MemberListEntry)*(entryList.size()) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    unsigned entryListSize = entryList.size();
    msg->msgType = HEARTBEAT;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr),&entryListSize,sizeof(unsigned));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr) + sizeof(unsigned), &entryList[0], 
                        sizeof(MemberListEntry)*entryList.size());

    
    // Send message to random btarget
    while( (btarget > 0) && (entryList.size() >0) )  {
            unsigned target = rand() % entryList.size();
            if(entryList[target].id != id) {
                targetAddress = getAddress(entryList[target].id, entryList[target].port);
                emulNet->ENsend(&memberNode->addr, &targetAddress, (char *)msg, msgsize);
                btarget--;
            }
            entryList.erase(entryList.begin() + target);
        }
    

    free(msg);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getAddress(int id, short port) {
 Address myAddr;

    memset(&myAddr, 0, sizeof(Address));
    *(int *)(&myAddr.addr) = id;
    *(short *)(&myAddr.addr[4]) = port;

    return myAddr;

}
/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
