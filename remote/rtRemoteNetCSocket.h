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

#ifndef __RT_REMOTE_NETWORK_RAW_SOCKET_H__
#define __RT_REMOTE_NETWORK_RAW_SOCKET_H__

#include "rtRemoteNet.h"


class rtRemoteCSocket : public rtRemoteSocket
{
public:
  rtRemoteCSocket();
  ~rtRemoteCSocket() override;
  rtError connect(rtRemoteAddress* remoteAddress) override;
  rtError disconnect() override;
  bool isConnected() override;
  rtError send(const char *message, size_t length) override;
  rtError send(rtRemoteMessagePtr const& msg) override;
  rtError accept(int fd);
  std::shared_ptr<rtRemoteCSocket> shared_from_this()
  {
      return shared_from(this);
  }
private:
  friend class rtRemoteCSocketClient;
  friend class rtRemoteCSocketServer;
  int m_fd;
};


class rtRemoteCSocketServer : public rtRemoteSocketServer
{
public:
  rtRemoteCSocketServer();
  ~rtRemoteCSocketServer() override;
  rtError start(std::shared_ptr<rtRemoteSocketServerListener> listener, int port = 0, const char* host = nullptr) override;
  rtError stop() override;
private:
  void onThreadRun() override;

  int m_listen_fd;
  int m_shutdown_pipe[2];
};


class rtRemoteCSocketFactory: public rtRemoteNetFactory
{
public:
  std::shared_ptr<rtRemoteSocket> createSocket() override;
  std::shared_ptr<rtRemoteSocketServer> createServer() override;
};

#endif
