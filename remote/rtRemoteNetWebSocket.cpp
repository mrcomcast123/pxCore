/*

pxCore Copyright 2005-2018 John Robinson

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

#include "rtRemoteNetWebSocket.h"
#include "rtRemoteSocketUtils.h"
#include "rtRemoteMessage.h"
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <queue>

/*****************************************************************************************
 * 
 * rtRemoteWebSocketClient
 *
*****************************************************************************************/

class rtRemoteWebSocketClient : public rtRemoteNetThread
{
public:
  static rtRemoteWebSocketClient* instance()
  {
    static rtRemoteWebSocketClient* pinstance = nullptr;
    if(!pinstance)
    {
      pinstance = new rtRemoteWebSocketClient();
      pinstance->start();
    }
    return pinstance;
  }
  rtError start();
  void connect(rtRemoteAddress* remoteAddress, rtRemoteWebSocket* socket);
  void connectNext();
protected:
  void onThreadRun() override;
private:
  std::queue<std::pair<std::string, rtRemoteWebSocket*> > m_connQueue;
  uWS::Hub m_hub;
  uS::Async* m_asyncConnect;
};

void connectNext2()
{
  rtRemoteWebSocketClient::instance()->connectNext();
}

rtError rtRemoteWebSocketClient::start()
{
  if(isThreadRunning())
  {
    printf("client already started\n");
    return RT_ERROR;
  }
  startThread();

  m_hub.onConnection([this](uWS::WebSocket<uWS::CLIENT> *ws, uWS::HttpRequest req) {
      printf("websocket connect\n");
      rtRemoteWebSocket* socket = (rtRemoteWebSocket*)ws->getUserData();
      socket->m_clientSocket = ws;
      //ws->setUserData(socket);
      //this->getListener()->onConnect(socket);
  });

  m_hub.onDisconnection([this](uWS::WebSocket<uWS::CLIENT> *ws, int code, char *message, size_t length) {
      printf("websocket disconnect\n");
      //rtRemoteWebSocket* socket = (rtRemoteWebSocket*)ws->getUserData();
      //this->getListener()->onDisconnect(socket);
      //delete socket;
  });

  m_hub.onMessage([this](uWS::WebSocket<uWS::CLIENT> *ws, char *message, size_t length, uWS::OpCode opCode) {
      printf("websocket message: %s\n", message);
      rtRemoteWebSocket* socket = (rtRemoteWebSocket*)ws->getUserData();

      rtRemoteMessagePtr doc = nullptr;
      rtError e = rtParseMessage(message, (int)length, doc);
      if(e == RT_OK)
      {
        e = socket->onMessage(doc);
      }
      if (e != RT_OK)
      {
        //TODO cleanup the socket
        rtLogWarn("error dispatching message. %s", rtStrError(e));
        //m_streams[i].reset();
        //s.reset();
      }

  });
  m_asyncConnect = new uS::Async(m_hub.getLoop());
  m_asyncConnect->start([](uS::Async *a) {
      printf("Async connect\n");
      connectNext2();
  });
  printf("client start ok\n");
  return RT_OK;
}

void rtRemoteWebSocketClient::connect(rtRemoteAddress* remoteEndPoint, rtRemoteWebSocket* socket)
{
  rtRemoteSocketAddress* sockAddr = dynamic_cast<rtRemoteSocketAddress*>(remoteEndPoint);
  printf("in connect\n");
  char uri[100];//TODO size matters
  sprintf(uri, "ws://%s:%d", sockAddr->getHost().c_str(), sockAddr->getPort());
  m_connQueue.push(std::make_pair(uri, socket));
  m_asyncConnect->send();
  //connectNext();
}

void rtRemoteWebSocketClient::connectNext()
{
  printf("in connectNext\n");
  if(!m_connQueue.empty())
  {
    std::pair<std::string, rtRemoteWebSocket*> p = m_connQueue.front();
    m_connQueue.pop();
    printf("connecting to %s\n", p.first.c_str());
    m_hub.connect(p.first, (void*)p.second);
  }
  
}

void rtRemoteWebSocketClient::onThreadRun()
{
  printf("client hub running\n");
  m_hub.run();
}


/*****************************************************************************************
 * 
 * rtRemoteWebSocketServer
 *
*****************************************************************************************/

rtRemoteWebSocketServer::rtRemoteWebSocketServer()
{
}

rtRemoteWebSocketServer::~rtRemoteWebSocketServer()
{
  stop();
}

rtError rtRemoteWebSocketServer::start(std::shared_ptr<rtRemoteSocketServerListener> listener, int port, const char* host)
{
  printf("WebSocket Server Starting\n");

  if(isThreadRunning())
  {
    rtLogError("rtRemoteSocketServer already started.");
    return RT_ERROR;
  }
  m_listener = listener;

  if(port == 0)
  {
    //TODO - get port from config
    port = 3010;
  }

  if(host == nullptr)
  {
    //TODO we can pass nullptr to m_hub but we should check Config for any specific host it may or may not have
  }

  if(!m_hub.listen(host, port))
  {
    printf("server failed to listen on port\n");
    return RT_ERROR;
  }

  printf("server listening at %s %d\n", host, port);

  m_hub.onConnection([this](uWS::WebSocket<uWS::SERVER> *ws, uWS::HttpRequest req) {
      printf("websocket connect\n");

      //
      // Create first instance of socket shared pointer. 
      // All other shared pointers should come from this one.
      //
      std::shared_ptr<rtRemoteWebSocket> socket = std::make_shared<rtRemoteWebSocket>();

      socket->accept(ws);

      connectSocket(socket, this);

      //stash a handle to quickly find later
      ws->setUserData(socket.get());
  });

  m_hub.onDisconnection([this](uWS::WebSocket<uWS::SERVER> *ws, int code, char *message, size_t length) {
      printf("websocket disconnect\n");

      rtRemoteWebSocket* socket = static_cast< rtRemoteWebSocket* >(ws->getUserData());
      disconnectSocket(socket);
  });

  m_hub.onMessage([this](uWS::WebSocket<uWS::SERVER> *ws, char *message, size_t length, uWS::OpCode opCode) {
      printf("websocket message: %s\n", message);

      rtRemoteWebSocket* socket = static_cast< rtRemoteWebSocket* >(ws->getUserData());

      rtRemoteMessagePtr doc = nullptr;
      rtError e = rtParseMessage(message, (int)length, doc);
      if(e == RT_OK)
      {
        e = socket->onMessage(doc);
      }
      if (e != RT_OK)
      {
        //TODO cleanup the socket
        rtLogWarn("error dispatching message. %s", rtStrError(e));
        //m_streams[i].reset();
        //s.reset();
      }

  });

  a = new uS::Async(m_hub.getLoop());
  a->start([](uS::Async *a) {
      printf("Async close\n");
      a->close();
  });

  m_address = std::make_shared<rtRemoteSocketAddress>(host != nullptr ? host : "localhost" , port, "ws");

  printf("server onStart ok\n");

  startThread();
  return RT_OK;
}

rtError rtRemoteWebSocketServer::stop()
{
  a->send();
  stopThread();
}

void rtRemoteWebSocketServer::onThreadRun()
{
  printf("server hub running\n");
  m_hub.run();
}


/*****************************************************************************************
 * 
 * rtRemoteWebSocket
 *
*****************************************************************************************/

rtRemoteWebSocket::rtRemoteWebSocket()
: rtRemoteSocket(),
  m_serverSocket(NULL),
  m_clientSocket(NULL)
{
}
/*
rtRemoteWebSocket::rtRemoteWebSocket(uWS::WebSocket<uWS::CLIENT>* s)
: rtRemoteSocket(),
  m_serverSocket(NULL),
  m_clientSocket(s)
{
}
*/
rtRemoteWebSocket::~rtRemoteWebSocket()
{
}

rtError rtRemoteWebSocket::connect(rtRemoteAddress* remoteAddress)
{
  if(isConnected())
  {
    disconnect();
  }

  rtRemoteWebSocketClient::instance()->connect(remoteAddress, this);

  while(m_clientSocket == 0)
    usleep(100);//TODO WMR fix this clunky thing.  need to also know connect failure error

  return RT_OK;
}

rtError rtRemoteWebSocket::accept(uWS::WebSocket<uWS::SERVER>* s)
{
  m_serverSocket = s;
}

rtError rtRemoteWebSocket::disconnect()
{
  return RT_OK;
}

bool rtRemoteWebSocket::isConnected()
{
  return m_clientSocket || m_serverSocket;
}

rtError rtRemoteWebSocket::send(const char *message, size_t length)
{
  if(m_serverSocket)
    m_serverSocket->send(message, length, uWS::OpCode::TEXT);
  else if(m_clientSocket)
    m_clientSocket->send(message, length, uWS::OpCode::TEXT);
  return RT_OK;//TODO WMR check errors
}

rtError rtRemoteWebSocket::send(rtRemoteMessagePtr const& msg)
{
  rapidjson::StringBuffer buff;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buff);
  msg->Accept(writer);
  if(m_serverSocket)
    m_serverSocket->send(buff.GetString(), buff.GetSize(), uWS::OpCode::TEXT);
  else if(m_clientSocket)
    m_clientSocket->send(buff.GetString(), buff.GetSize(), uWS::OpCode::TEXT);
  return RT_OK;//TODO WMR check errors
}


/*****************************************************************************************
 * 
 * rtRemoteWebSocketFactory
 *
*****************************************************************************************/

std::shared_ptr<rtRemoteSocket> rtRemoteWebSocketFactory::createSocket()
{
  TRACE_FUNC
  return std::static_pointer_cast<rtRemoteSocket>(std::make_shared<rtRemoteWebSocket>());
}

std::shared_ptr<rtRemoteSocketServer> rtRemoteWebSocketFactory::createServer()
{
  TRACE_FUNC
  return std::static_pointer_cast<rtRemoteSocketServer>(std::make_shared<rtRemoteWebSocketServer>());
}

