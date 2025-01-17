//
// Copyright (C) 2000 Institut fuer Telematik, Universitaet Karlsruhe
// Copyright (C) 2004,2011 Andras Varga
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, see <http://www.gnu.org/licenses/>.
//

#ifndef __INET_MQTTAPP_H
#define __INET_MQTTAPP_H

#include <vector>

#include "inet/common/INETDefs.h"

#include "inet/applications/base/ApplicationBase.h"
#include "inet/transportlayer/contract/udp/UDPSocket.h"
#include "inet/applications/mqttapp/mqttMessages/MqttMsgPublish_m.h"
#include "PublishedData.cc"

namespace inet {

/**
 * UDP application. See NED for more info.
 */
class INET_API mqttApp : public ApplicationBase
{
  protected:
    enum SelfMsgKinds { START = 1, SEND, STOP, MQTTDATA, MQTTSUBSCRIBED };

    // parameters
    std::vector<L3Address> destAddresses;
    std::string clientID;
    int localPort = -1, destPort = -1;
    simtime_t startTime;
    simtime_t stopTime;
    const char *packetName = nullptr;
    int ptrSubscribe;
    int prtPublish;
    bool isConnected;
    bool publish;

    std::vector<PublishedData *> publishedDatatable;
    std::vector<MqttMsgPublish *> publishedRequests;
    std::vector<std::string> subscribeToTopics;
    std::vector<std::string> publishToTopics;


    // state
    UDPSocket socket;
    cMessage *selfMsg = nullptr;

    // statistics
    int numSent = 0;
    int numReceived = 0;

    static simsignal_t delaySignal;

    static simsignal_t sentPkSignal;
    static simsignal_t rcvdPkSignal;

  protected:
    virtual int numInitStages() const override { return NUM_INIT_STAGES; }
    virtual void initialize(int stage) override;
    virtual void handleMessageWhenUp(cMessage *msg) override;
    virtual void finish() override;

    // chooses random destination address
    virtual L3Address chooseDestAddr();
    virtual void sendPacket();
    virtual void processPacket(cPacket *msg);
    virtual void setSocketOptions();

    virtual void processStart();
    virtual void processSend();
    virtual void processStop();
    virtual void sendMqttData();
    virtual void processConSubAck(cPacket *c);

    virtual void sendConnect();

    virtual bool handleNodeStart(IDoneCallback *doneCallback) override;
    virtual bool handleNodeShutdown(IDoneCallback *doneCallback) override;
    virtual void handleNodeCrash() override;
    std::string RandomString(unsigned int len);

  public:
    mqttApp() {}
    ~mqttApp();
};

} // namespace inet

#endif // ifndef __INET_UDPBASICAPP_H

