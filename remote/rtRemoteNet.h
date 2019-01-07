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

#ifndef __RT_REMOTE_NETWORK_H__
#define __RT_REMOTE_NETWORK_H__

#include <string>
#include <memory>
#include <stdint.h>
#include <thread>
#include <sstream>
#include <string.h>
#include <list>
#include <sys/socket.h>
#include "rtError.h"
#include "rtLog.h"
#include "rtRemoteMessage.h"


class rtRemoteFuncTracer
{
public:
  rtRemoteFuncTracer(const char* func) : m_func(func)
  {
    printf("%s enter\n", m_func.c_str());
  }
  ~rtRemoteFuncTracer()
  {
    printf("%s exit\n", m_func.c_str());
  }
  std::string m_func;
};

#define TRACE_FUNC 
//#define TRACE_FUNC rtRemoteFuncTracer my_func_tracer_(__PRETTY_FUNCTION__);

class rtRemoteAddress
{
public:
  virtual ~rtRemoteAddress(){}
  rtRemoteAddress(const std::string& scheme);
  const std::string& getScheme() const;
  static std::shared_ptr<rtRemoteAddress> fromString(std::string& s);//TODO WMR should fromString return shared_ptr because it does a new malloc inside
  virtual std::string toString() = 0;
  virtual sockaddr_storage toSockAddr() const = 0;
  static bool compare(rtRemoteAddress* left, rtRemoteAddress* right);
protected:
  rtRemoteAddress();
  std::string m_scheme;
};

using rtRemoteAddressPtr = std::shared_ptr< rtRemoteAddress >;


/* Local endpoints.
 * Used to stored address information for unix domain sockets,
 * named pipes, files, shared memory, etc.
 */
class rtRemoteFileAddress : public virtual rtRemoteAddress
{
public:
  rtRemoteFileAddress(std::string const& scheme, std::string const& path);
  static std::shared_ptr<rtRemoteFileAddress> fromString(std::string const& s);
  static std::shared_ptr<rtRemoteFileAddress> fromSockAddr(sockaddr_storage const& s);
  virtual sockaddr_storage toSockAddr() const override;
  virtual std::string toString() override;
  bool operator == (rtRemoteFileAddress const& rhs) const
  {
    return m_path.compare(rhs.getPath()) == 0 && m_scheme.compare(rhs.getScheme()) == 0; 
  }
  std::string const& getPath() const
  {
    return m_path; 
  }
private:
  rtRemoteFileAddress();

protected:
  std::string m_path;
};


/* Remote endpoints.
 * Used to stored address information for remote sockets
 * (tcp, udp, http, etc.)
 */
class rtRemoteSocketAddress : public rtRemoteAddress
{
public:
  rtRemoteSocketAddress(const std::string& host, int port, const std::string& scheme = "");
  static std::shared_ptr<rtRemoteSocketAddress> fromString(std::string const& s);
  static std::shared_ptr<rtRemoteSocketAddress> fromSockAddr(std::string const& scheme, sockaddr_storage const& ss);
  virtual sockaddr_storage toSockAddr() const override;
  virtual std::string toString() override;
  bool operator == (rtRemoteSocketAddress const& rhs) const
  {
    return m_host.compare(rhs.getHost()) == 0
     && m_port == rhs.getPort()
     && m_scheme.compare(rhs.getScheme()) == 0;
  }
  std::string getHost() const
  {
    return m_host;
  }
  int getPort() const
  {
    return m_port;
  }
protected:
  rtRemoteSocketAddress();
private:
  std::string m_host;
  int m_port;
};


enum rtRemoteSocketState
{
  rtRemoteSocketStateOpened,
  rtRemoteSocketStateInactive,
  rtRemoteSocketStateClosed
};

class rtRemoteSocket;

class rtRemoteSocketListener
{
public:
  virtual ~rtRemoteSocketListener(){}
  virtual rtError onMessage(std::shared_ptr<rtRemoteSocket> const& socket, rtRemoteMessagePtr const& doc) = 0;
  virtual rtError onStateChanged(std::shared_ptr<rtRemoteSocket> const& socket, rtRemoteSocketState state) = 0;
};

template <typename Base>
inline std::shared_ptr<Base>
shared_from_base(std::enable_shared_from_this<Base>* base) 
{
    return base->shared_from_this();
}
template <typename Base>
inline std::shared_ptr<const Base>
shared_from_base(std::enable_shared_from_this<Base> const* base) 
{
    return base->shared_from_this();
}
template <typename That>
inline std::shared_ptr<That>
shared_from(That* that) 
{
    return std::static_pointer_cast<That>(shared_from_base(that));
}

class rtRemoteSocketServer;

class rtRemoteSocket : public std::enable_shared_from_this<rtRemoteSocket>
{
public:
  rtRemoteSocket();
  virtual ~rtRemoteSocket(){ /* TODO should we call disconnect() here ? */}
  virtual rtError connect(rtRemoteAddress* remoteAddress) = 0;
  virtual rtError send(const char *message, size_t length) = 0;
  virtual rtError send(rtRemoteMessagePtr const& msg) = 0;
  virtual rtError disconnect() = 0;
  virtual bool isConnected() = 0;
  rtError setListener(std::shared_ptr<rtRemoteSocketListener> const& listener);
  std::weak_ptr<rtRemoteSocketListener> const& getListener()
  {
    return m_listener;
  }
  rtError onMessage(rtRemoteMessagePtr doc);
  rtError onInactivity();
  rtError onDisconnect();
  void setServer(rtRemoteSocketServer* s){ m_server = s; }
  std::shared_ptr<rtRemoteAddress> getAddress(){ return m_address; }
protected:
  std::weak_ptr<rtRemoteSocketListener> m_listener;
  rtRemoteSocketServer* m_server;
  std::shared_ptr<rtRemoteAddress> m_address;
};


class rtRemoteSocketServerListener
{
public:
  virtual ~rtRemoteSocketServerListener(){}
  virtual void onConnect(std::shared_ptr<rtRemoteSocket> socket) = 0;
  virtual void onDisconnect(std::shared_ptr<rtRemoteSocket> socket) = 0;
};


class rtRemoteNetThread
{
public:
  rtRemoteNetThread();
  virtual ~rtRemoteNetThread();
protected:
  void startThread();
  void stopThread();
  bool isThreadRunning()
  {
    return m_thread != nullptr;
  }
  virtual void onThreadRun() = 0;

private:
  static void runThread(rtRemoteNetThread* self);
  std::unique_ptr<std::thread> m_thread;
};


class rtRemoteSocketServer : public rtRemoteNetThread
{
public:
  virtual rtError start(std::shared_ptr<rtRemoteSocketServerListener> listener, int port = 0, const char* host = nullptr) = 0;
  virtual rtError stop() = 0;
  std::list< std::shared_ptr<rtRemoteSocket> > const& getSockets(){ return m_sockets; }
  std::shared_ptr<rtRemoteAddress> getAddress(){ return m_address; }
protected:
  void connectSocket(std::shared_ptr<rtRemoteSocket> socket, rtRemoteSocketServer* server=nullptr);
  void disconnectSocket(std::shared_ptr<rtRemoteSocket> socket);
  void disconnectSocket(rtRemoteSocket* socket);

  std::weak_ptr<rtRemoteSocketServerListener> m_listener;
  std::list< std::shared_ptr<rtRemoteSocket> > m_sockets;
  std::shared_ptr<rtRemoteAddress> m_address;
  friend class rtRemoteSocket;
};


enum rtRemoteNetType
{
  rtRemoteNetTypeDefault,
  rtRemoteNetTypeCSocket,
  rtRemoteNetTypeWebSocket
};

class rtRemoteNetFactory
{
public:
  static std::unique_ptr<rtRemoteNetFactory>& get(rtRemoteNetType type = rtRemoteNetTypeDefault);
  static std::unique_ptr<rtRemoteNetFactory>& get(std::shared_ptr<rtRemoteAddress> addressHint);
  virtual ~rtRemoteNetFactory(){}
  virtual std::shared_ptr<rtRemoteSocket> createSocket() = 0;
  virtual std::shared_ptr<rtRemoteSocketServer> createServer() = 0;
};

/*
class rtStreamListener
{
public:
  virtual rtError onMessage(rtRemoteMessagePtr const& doc) = 0;
  virtual rtError onStateChanged(rtRemoteStream* stream, State state) = 0;
};

class rtRemoteStream
{
public:
  virtual ~rtRemoteStream(){}
  virtual rtError createFromSocket(rtRemoteIEndPoint* endPoint) = 0;
  virtual rtError createExisting(rtRemoteNetworkConnection* connection) = 0;

  class CallbackHandler {
  public:
    virtual ~CallbackHandler() { }
    virtual rtError onMessage(rtRemoteMessagePtr const& doc) = 0;
    virtual rtError onStateChanged(std::shared_ptr<rtRemoteStream> const& stream, State state) = 0;
  };
  rtError setCallbackHandler(std::shared_ptr<CallbackHandler> const& callbackHandler)

private:
  std::weak_ptr<CallbackHandler> m_callback_handler;
};
*/

#endif
