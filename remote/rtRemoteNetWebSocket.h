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

#ifndef __RT_REMOTE_NETWORK_WEB_SOCKET_H__
#define __RT_REMOTE_NETWORK_WEB_SOCKET_H__

#include "rtRemoteNet.h"
#include <uWS.h>


class rtRemoteWebSocket : public rtRemoteSocket
{
public:
  rtRemoteWebSocket();
  ~rtRemoteWebSocket() override;
  rtError connect(rtRemoteAddress* remoteAddress) override;
  rtError accept(uWS::WebSocket<uWS::SERVER>* s);
  rtError disconnect() override;
  bool isConnected() override;
  rtError send(const char *message, size_t length) override;
  rtError send(rtRemoteMessagePtr const& msg) override;
private:
  friend class rtRemoteWebSocketClient;
  friend class rtRemoteWebSocketServer;
  uWS::WebSocket<uWS::SERVER>* m_serverSocket;
  uWS::WebSocket<uWS::CLIENT>* m_clientSocket;
};


class rtRemoteWebSocketServer : public rtRemoteSocketServer
{
public:
  rtRemoteWebSocketServer();
  ~rtRemoteWebSocketServer() override;
  rtError start(std::shared_ptr<rtRemoteSocketServerListener> listener, int port = 0, const char* host = nullptr) override;
  rtError stop() override;
private:
  void onThreadRun() override;

  uWS::Hub m_hub;
  uS::Async* a;
};


class rtRemoteWebSocketFactory: public rtRemoteNetFactory
{
public:
  std::shared_ptr<rtRemoteSocket> createSocket() override;
  std::shared_ptr<rtRemoteSocketServer> createServer() override;
};

#endif
