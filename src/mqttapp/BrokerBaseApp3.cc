/*
 * BrokerBaseApp3.cc
 *
 *  Created on: May 16, 2018
 *      Author: compute1
 */

#include "inet/applications/mqttapp/BrokerBaseApp3.h"
#include "inet/common/ModuleAccess.h"
#include "inet/transportlayer/contract/udp/UDPControlInfo_m.h"
#include "inet/applications/mqttapp/mqttMessages/MqttMsgConnack_m.h"
#include "inet/transportlayer/contract/udp/UDPSocket.h"
#include "inet/applications/mqttapp/mqttMessages/MqttMsgConnect_m.h"
#include "inet/applications/mqttapp/mqttMessages/MqttMsgSuback_m.h"
#include "inet/applications/mqttapp/mqttMessages/MqttMsgSubscribe_m.h"
#include "inet/applications/mqttapp/mqttMessages/MqttMsgPublish_m.h"
#include "inet/applications/mqttapp/mqttMessages/MqttMsgPuback_m.h"
#include "inet/applications/mqttapp/fognetMessages/FognetMsgAdvertiseMIPS_m.h"
#include "inet/applications/mqttapp/fognetMessages/FognetMsgTask_m.h"
#include "inet/applications/mqttapp/fognetMessages/FognetMsgTaskAck_m.h"

#include "Client.cc"
#include "Broker.cc"
#include "Subscription.cc"

namespace inet {

Define_Module(BrokerBaseApp3);

simsignal_t BrokerBaseApp3::pkSignal = registerSignal("pk");
simsignal_t BrokerBaseApp3::delaySignal = registerSignal("delay");

void BrokerBaseApp3::initialize(int stage)
{
    ApplicationBase::initialize(stage);

    if (stage == INITSTAGE_LOCAL) {

        numClients=0;
        MIPS=par("MIPS");
        algo=par("algo");
        WATCH(numClients);
        numSubscribed=0;

        // init statistics
        numEchoed = 0;

        WATCH(numEchoed);


        selfMsg = new cMessage("sendTimer");

        selfMsg->setKind(10);

    }

    else if (stage == INITSTAGE_LAST) {
        if (hasGUI())
            updateDisplay();
    }
}

void BrokerBaseApp3::handleMessageWhenUp(cMessage *msg)
{

    if (msg->isSelfMessage()) {
        ASSERT(msg == selfMsg);
        switch (selfMsg->getKind()) {

        case RELEASERESOURCE:
            releaseResource(msg->getName());
            break;
        default:
            throw cRuntimeError("Invalid kind %d in self message", (int)selfMsg->getKind());
        }
    }

    else if (msg->getKind() == UDP_I_ERROR) {
        delete msg;
    }

    else if(msg->getKind()==9){
        publishAll(msg);
    }

    else if (msg->getKind() == UDP_I_DATA) {

        cPacket *pk = PK(msg);
        // statistics
        numEchoed++;
        emit(pkSignal, pk);
        // determine its source address/port

        UDPDataIndication *ctrl = check_and_cast<UDPDataIndication *>(pk->removeControlInfo());
        L3Address srcAddress = ctrl->getSrcAddr();
        int srcPort = ctrl->getSrcPort();
        delete ctrl;

        if (dynamic_cast<MqttMsgConnect* >(msg)) {
            MqttMsgConnect *mqttPkt = check_and_cast<MqttMsgConnect *>(msg);

            if(mqttPkt->getIsBroker()==true){

                Broker *broker=new Broker(mqttPkt->getClientId(),srcAddress,srcPort,0);
                brokers.push_back(broker);
                EV<<"brokers: "<<brokers.size();
            }

            else{
                Client *client=new Client(mqttPkt->getClientId(),srcAddress,srcPort,mqttPkt->getQosLevel(),
                        mqttPkt->getProtocolName(),mqttPkt->getProtocolVersion(),mqttPkt->getWillFlag(),
                        mqttPkt->getWillTopic(), mqttPkt->getWillMessage(),mqttPkt->getCleanSession(),
                        mqttPkt->getKeepAlivePeriod());
                clients.push_back(client);

                numClients++;
            }

            MqttMsgConnack *rply=new MqttMsgConnack("mqttConnack");
            socket.sendTo(rply, srcAddress, srcPort);
        }

        else if (dynamic_cast<FognetMsgAdvertiseMIPS* >(msg)){
            FognetMsgAdvertiseMIPS *advertisement = check_and_cast<FognetMsgAdvertiseMIPS *>(msg);
            for(unsigned int j=0; j< brokers.size(); j++){
                if(strcmp(brokers[j]->getBrokerId(), advertisement->getComputeBrokerID())==0){
                    brokers[j]->setMips(advertisement->getMIPS());
                    brokers[j]->setBusyTime(advertisement->getBusyTime());
                }
            }

            //test
            for(unsigned int u=0;u<brokers.size();u++){
                EV<<"busy time of "<<brokers[u]->getBrokerId() <<" is "<<brokers[u]->getBusyTime()<<" \n";
            }
        }

        else if (dynamic_cast<MqttMsgPublish* >(msg)) {

            MqttMsgPublish *mqttPkt = check_and_cast<MqttMsgPublish *>(msg);

            if(mqttPkt->getQoS()==1){
                emit(delaySignal, ((simTime()-mqttPkt->getCreationTime())));

                std::ostringstream str;
                str << "Ack" << "-" << mqttPkt->getName()<<"-compp requested-"<<mqttPkt->getMessageID();
                MqttMsgPuback *pubAck=new MqttMsgPuback(str.str().c_str());
                pubAck->setMessageID(mqttPkt->getMessageID());
                pubAck->setStatus(4); //request forwarded to compute broker
                socket.sendTo(pubAck, srcAddress, srcPort);
                sendPubAck(mqttPkt,srcAddress,srcPort, false);

            }
            else if(mqttPkt->getQoS()==0){

            }

        }

        else if(dynamic_cast<FognetMsgTaskAck* >(msg)){

        }

        else if(dynamic_cast<MqttMsgPuback* >(msg)){
            MqttMsgPuback *ack = check_and_cast<MqttMsgPuback *>(msg);
            if(ack->getStatus()==6){
                for(unsigned int a=0;a<requests.size();a++){
                    if(strcmp(requests[a]->getRequestId(),ack->getMessageID())==0){
                        socket.sendTo(ack->dup(),requests[a]->getClientIp(),requests[a]->getClientPort());
                        requests[a]->setStatus(true);
                        requests[a]->setAckStatus(1);
                        break;
                    }
                }
            }
            //if task is assigned
            else if(ack->getStatus()==5){
                for(unsigned int a=0;a<requests.size();a++){
                    if(strcmp(requests[a]->getRequestId(),ack->getMessageID())==0){
                        socket.sendTo(ack->dup(),requests[a]->getClientIp(),requests[a]->getClientPort());
                        requests[a]->setStatus(false);
                        requests[a]->setAckStatus(1);
                        break;
                    }
                }
            }
            //if task is queued
            else if(ack->getStatus()==4){
                for(unsigned int a=0;a<requests.size();a++){
                    if(strcmp(requests[a]->getRequestId(),ack->getMessageID())==0){
                        socket.sendTo(ack->dup(),requests[a]->getClientIp(),requests[a]->getClientPort());
                        requests[a]->setStatus(false);
                        requests[a]->setAckStatus(1);
                        break;
                    }
                }
            }
        }


        else if (dynamic_cast<MqttMsgSubscribe* >(msg)) {

            EV<<"broker break3";

            MqttMsgSubscribe *mqttPkt = check_and_cast<MqttMsgSubscribe *>(msg);
            Subscription *sub=new Subscription(mqttPkt->getClientID(), mqttPkt->getQos(),mqttPkt->getTopic());
            subscriptions.push_back(sub);
            for(unsigned int i=0;i<subscriptions.size();i++){
                EV_WARN<<"Subscribed with client ID: "<<subscriptions[i]->getClientId()<<", Topic: "<<subscriptions[i]->getTopic() <<"\n";
            }
            numSubscribed++;

            std::ostringstream ackName;
            ackName << "subAck" << "-" << mqttPkt->getTopic();

            MqttMsgSuback * mqttSubAck=new MqttMsgSuback(ackName.str().c_str());
            socket.sendTo(mqttSubAck, srcAddress, srcPort);
        }



        if (hasGUI())
            updateDisplay();
    }
    else {
        throw cRuntimeError("Message received with unexpected message kind = %d", msg->getKind());
    }
}


void BrokerBaseApp3::sendPubAck(MqttMsgPublish *mqttPkt, L3Address ip, int prt, bool status){

    std::ostringstream str;
    double tskTime=0.0;

    if(status==true){
        MIPS=MIPS - mqttPkt->getMIPSRequired();
        tskTime=((static_cast<double>(mqttPkt->getByteLength()))/MIPS);

        Request *req=new Request(mqttPkt->getClientID(),mqttPkt->getMessageID(),
                ip,prt,mqttPkt->getMIPSRequired(),tskTime,false);
        req->setRequestId(mqttPkt->getMessageID());

        requests.push_back(req);

        str << "Ack" << "-" << mqttPkt->getName()<<"-processing task-"<<mqttPkt->getMessageID();
        MqttMsgPuback *pubAck=new MqttMsgPuback(str.str().c_str());
        pubAck->setMessageID(mqttPkt->getMessageID());
        pubAck->setStatus(3);


        for(unsigned int i=0; i< clients.size(); i++){
            if(strcmp(mqttPkt->getClientID(),clients[i]->getClientId())==0){
                socket.sendTo(pubAck, clients[i]->getClientIp(), clients[i]->getClientPort());
                cancelEvent(selfMsg);
                selfMsg->setKind(RELEASERESOURCE);
                selfMsg->setName(mqttPkt->getMessageID());
                scheduleAt(simTime() + mqttPkt->getRequiredTime(), selfMsg);
                break;
            }
        }

    }

    else{

        int currentGoodBroker=0;
        tskTime=mqttPkt->getMIPSRequired()/brokers[0]->getMips();
        if(brokers.size() > 0){
            double tempp=(brokers[0]->getBusyTime())+tskTime;
            if(brokers.size()>1){
                for(unsigned int j=0;j<brokers.size(); j++){
                    if((brokers[j]->getBusyTime()+ (mqttPkt->getMIPSRequired()/brokers[0]->getMips()) )<tempp){
                       
                        tempp=brokers[j]->getBusyTime()+(mqttPkt->getMIPSRequired()/brokers[0]->getMips());

                        currentGoodBroker=j;
                    }
                }

            }

            std::ostringstream str;
            str << "request "<<mqttPkt->getMIPSRequired() <<" for "<< mqttPkt->getRequiredTime()<<" sec from " << brokers[currentGoodBroker]->getBrokerId();
            FognetMsgTask *tsk=new FognetMsgTask(str.str().c_str());

            tsk->setByteLength(mqttPkt->getByteLength());

            Request *req=new Request(mqttPkt->getClientID(),mqttPkt->getMessageID(),
                    ip,prt,mqttPkt->getMIPSRequired(),
                    simTime().dbl() + mqttPkt->getRequiredTime(),false);
            req->setRequestId(mqttPkt->getMessageID());

            requests.push_back(req);

            tsk->setRequiredMIPS(mqttPkt->getMIPSRequired());
            tsk->setRequiredTime(mqttPkt->getRequiredTime());
            tsk->setRequestID(mqttPkt->getMessageID());
            std::string s = std::to_string(this->getId());
            char const *pchar = s.c_str();
            tsk->setClientID(pchar);
            socket.sendTo(tsk, brokers[currentGoodBroker]->getBrokerIp(), brokers[currentGoodBroker]->getBrokerPort());
           
        }

        else{
            MqttMsgPuback *pubAck=new MqttMsgPuback("no compute resource available");

            pubAck->setMessageID("rep");

            for(unsigned int i=0; i< clients.size(); i++){
                if(strcmp(mqttPkt->getClientID(),clients[i]->getClientId())==0){
                    socket.sendTo(pubAck, clients[i]->getClientIp(), clients[i]->getClientPort());
                    selfMsg->setKind(RELEASERESOURCE);
                    scheduleAt(simTime() + mqttPkt->getRequiredTime(), selfMsg);
                    break;
                }
            }
        }

    }
}

void BrokerBaseApp3::updateDisplay()
{
    char buf[40];
    sprintf(buf, "echoed: %d pks", numEchoed);
    getDisplayString().setTagArg("t", 0, buf);
}

void BrokerBaseApp3::finish()
{
    recordScalar("delay", delaySignal);
    ApplicationBase::finish();
}

bool BrokerBaseApp3::handleNodeStart(IDoneCallback *doneCallback)
{
    socket.setOutputGate(gate("udpOut"));
    int localPort = par("localPort");
    socket.bind(localPort);
    MulticastGroupList mgl = getModuleFromPar<IInterfaceTable>(par("interfaceTableModule"), this)->collectMulticastGroups();
    socket.joinLocalMulticastGroups(mgl);
    return true;
}

bool BrokerBaseApp3::handleNodeShutdown(IDoneCallback *doneCallback)
{
    return true;
}

void BrokerBaseApp3::handleNodeCrash()
{
}
Client* BrokerBaseApp3::findClient(const char* clientID){
    Client* c=new Client();
    for(int i=0; i<10; i++){
        if(strcmp(clientID, clients[i]->getClientId())==0){
            return clients[i];
        }
    }
    return c;
}

void BrokerBaseApp3::publishAll(cMessage *msg){

    MqttMsgPublish *mqttPkt = check_and_cast<MqttMsgPublish *>(msg);

    for(unsigned int j=0; j<subscriptions.size(); j++){
        if(strcmp(mqttPkt->getTopic(), subscriptions[j]->getTopic())==0){
            MqttMsgPublish *copy ;
            copy= mqttPkt->dup();
            std::stringstream str;
            str <<"pub back: "<< mqttPkt->getTopic();

            copy->setName(str.str().c_str());

            for(unsigned int a=0; a < clients.size(); a++){
                if(strcmp(subscriptions[j]->getClientId(),clients[a]->getClientId())==0){
                    socket.sendTo(copy, clients[a]->getClientIp(), clients[a]->getClientPort());
                }
            }
        }
    }
}

void BrokerBaseApp3::publishMessage(const char* clientID, cMessage *msg){
    MqttMsgPublish *mqttPkt = check_and_cast<MqttMsgPublish *>(msg);

    for (unsigned int i = 0; i < clients.size(); i++)
    {
        if(strcmp(clientID, mqttPkt->getClientID()) == 0){
            cPacket *copy = mqttPkt->dup();
            socket.sendTo(copy, clients[i]->getClientIp(), clients[i]->getClientPort());
        }

    }
    delete msg;
}

std::string BrokerBaseApp3::RandomString(unsigned int len)

{
    srand(time(0));
    std::string str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string newstr;
    int pos;
    while(newstr.size() != len) {
        pos = ((rand() % (str.size() - 1)));
        newstr += str.substr(pos,1);
    }
    return newstr;
}

void BrokerBaseApp3 ::releaseResource(const char* id){
    MqttMsgPuback *pubak=new MqttMsgPuback();
    for(unsigned int i=0; i < requests.size(); i++){
        if(requests[i]->getRequiredTime() <= simTime().dbl()){
            MIPS=MIPS + requests[i]->getRequiredMips(); //releasing the resource

            std::ostringstream str;
            str << "Task  " << requests[i]->getRequestId() << " performed";
            pubak->setName(str.str().c_str());

            pubak->setQos(0);
            pubak->setMessageID(requests[i]->getRequestId());
            pubak->setStatus(6);
            socket.sendTo(pubak,requests[i]->getClientIp(), requests[i]->getClientPort());
            requests.erase(requests.begin()+ (i)); //remove request/task from table
            EV<<"base broker tasks len: "<<requests.size(); //test
            break;
        }

    }

    //test

    for(unsigned int i=0; i < requests.size(); i++){
        EV<<"Task: "<<requests[i]->getRequestId()<<"\n";
    }

}

} // namespace inet
