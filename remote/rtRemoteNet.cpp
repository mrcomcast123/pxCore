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

#include "rtRemoteNet.h"
#include "rtRemoteNetCSocket.h"
#include "rtRemoteNetWebSocket.h"
#include "rtRemoteSocketUtils.h"

/*****************************************************************************************
 * 
 * rtRemoteAddress
 *
*****************************************************************************************/

rtRemoteAddress::rtRemoteAddress(const std::string& scheme) 
: m_scheme(scheme)
{
}

const std::string& rtRemoteAddress::getScheme() const
{
  return m_scheme;
}

std::shared_ptr<rtRemoteAddress> rtRemoteAddress::fromString(std::string& s)
{
  if (strncmp(s.c_str(), "unix", 4) == 0)
  {
    return rtRemoteFileAddress::fromString(s);
  }
  else if ((strncmp(s.c_str(), "tcp", 3) == 0) ||
           (strncmp(s.c_str(), "udp", 3) == 0) || 
           (strncmp(s.c_str(), "mcast", 4) == 0) ||
           (strncmp(s.c_str(), "ws", 2) == 0))
  {
    return rtRemoteSocketAddress::fromString(s);
  }

  return nullptr;
}

rtRemoteAddress::rtRemoteAddress()
{
}

bool rtRemoteAddress::compare(rtRemoteAddress* left, rtRemoteAddress* right)
{
  //TODO - fully test this
  std::string sleft = left->toString();
  std::string sright = right->toString();
  rtLogWarn("rtRemoteAddress::compare left=\"%s\" right=\"%s\" \n", sleft.c_str(), sright.c_str());
  return left->toString() == right->toString();
}


/*****************************************************************************************
 * 
 * rtRemoteFileAddress
 *
*****************************************************************************************/

rtRemoteFileAddress::rtRemoteFileAddress(std::string const& scheme, std::string const& path)
: rtRemoteAddress(scheme)
, m_path(path)
{
}

std::shared_ptr<rtRemoteFileAddress> rtRemoteFileAddress::fromString(std::string const& s)
{
  std::string::size_type begin, end;
  begin = 0;
  end = s.find("://");
  if (end == std::string::npos)
  {
    rtLogWarn("invalid fromString %s", s.c_str());
    return nullptr;
  }
  std::string scheme = s.substr(begin, end);
  std::string path = s.substr(end + 3);
  return std::make_shared<rtRemoteFileAddress>(scheme, path);
}

std::shared_ptr<rtRemoteFileAddress> rtRemoteFileAddress::fromSockAddr(sockaddr_storage const& s)
{
  void* addr = nullptr;
  rtGetInetAddr(s, &addr);
  return std::make_shared<rtRemoteFileAddress>("unix", reinterpret_cast<char const *>(addr));
}

sockaddr_storage rtRemoteFileAddress::toSockAddr() const
{
  sockaddr_storage s;
  memset(&s, 0, sizeof(s));
  rtParseAddress(s, m_path.c_str(), 0, nullptr);
  return s;
}

std::string rtRemoteFileAddress::toString()
{
  std::stringstream buff;
  buff << m_scheme;
  buff << "://";
  buff << m_path;
  return buff.str();
}

rtRemoteFileAddress::rtRemoteFileAddress() : rtRemoteAddress()
{
}


/*****************************************************************************************
 * 
 * rtRemoteSocketAddress
 *
*****************************************************************************************/

rtRemoteSocketAddress::rtRemoteSocketAddress(const std::string& host, int port, const std::string& scheme) 
  : rtRemoteAddress(scheme), 
    m_host(host), 
    m_port(port)
{
}

std::shared_ptr<rtRemoteSocketAddress> rtRemoteSocketAddress::fromString(std::string const& s)
{
  // <scheme>://<ip>:port
  std::string::size_type begin, end;
  
  begin = 0;
  end = s.find("://");
  if (end == std::string::npos)
  {
    rtLogWarn("invalid fromString %s", s.c_str());
    return nullptr;
  }

  std::string scheme = s.substr(begin, end);

  begin = end + 3;
  end = s.find(':', begin);
  if (end == std::string::npos)
  {
    rtLogWarn("invalid fromString %s", s.c_str());
    return nullptr;
  }
  std::string host = s.substr(begin, (end - begin));
  int port = std::stoi(s.substr(end + 1));

  return std::make_shared<rtRemoteSocketAddress>(host, port, scheme);
}

std::shared_ptr<rtRemoteSocketAddress> rtRemoteSocketAddress::fromSockAddr(std::string const& scheme, sockaddr_storage const& ss)
{
  socklen_t len;
  rtSocketGetLength(ss, &len);

  uint16_t port;
  rtGetPort(ss, &port);

  void* addr = nullptr;
  rtGetInetAddr(ss, &addr);

  char buff[256];
  memset(buff, 0, sizeof(buff));
  char const* p = inet_ntop(ss.ss_family, addr, buff, len);

  return std::make_shared<rtRemoteSocketAddress>(p, port, scheme);
}

sockaddr_storage rtRemoteSocketAddress::toSockAddr() const
{
  sockaddr_storage s;
  memset(&s, 0, sizeof(s));
  rtParseAddress(s, m_host.c_str(), m_port, nullptr);
  return s;
}

std::string rtRemoteSocketAddress::toString()
{
  std::stringstream buff;
  buff << m_scheme;
  buff << "://";
  buff << m_host;
  buff << ":";
  buff << m_port;
  return buff.str();
}

rtRemoteSocketAddress::rtRemoteSocketAddress()
{
}


/*****************************************************************************************
 * 
 * rtRemoteSocket
 *
*****************************************************************************************/

rtRemoteSocket::rtRemoteSocket() 
: m_server(nullptr)
{
}

rtError rtRemoteSocket::setListener(std::shared_ptr<rtRemoteSocketListener> const& listener)
{
  TRACE_FUNC
  m_listener = listener;
  return RT_OK;
}

rtError rtRemoteSocket::onMessage(rtRemoteMessagePtr doc)
{
  TRACE_FUNC
  std::shared_ptr<rtRemoteSocketListener> listener = m_listener.lock();
  if (listener)
  {
    return listener->onMessage(shared_from_this(), doc);
  }
  else
  return RT_OK;
}

rtError rtRemoteSocket::onInactivity()
{
  TRACE_FUNC
  std::shared_ptr<rtRemoteSocketListener> listener = m_listener.lock();
  if (listener)
  {
    return listener->onStateChanged(shared_from_this(), rtRemoteSocketStateInactive);
  }
  return RT_OK;
}

rtError rtRemoteSocket::onDisconnect()
{
  TRACE_FUNC

  std::shared_ptr<rtRemoteSocketListener> listener = m_listener.lock();

  if (listener)
  {
    listener->onStateChanged(shared_from_this(), rtRemoteSocketStateClosed);
  }

  if(m_server)
    m_server->disconnectSocket(this);

  return RT_OK;
}


/*****************************************************************************************
 * 
 * rtRemoteNetThread
 *
*****************************************************************************************/

rtRemoteNetThread::rtRemoteNetThread()
{
  TRACE_FUNC
  m_thread.reset(nullptr);
}

rtRemoteNetThread::~rtRemoteNetThread()
{
  TRACE_FUNC
  if(isThreadRunning())
    stopThread();
}

void rtRemoteNetThread::startThread()
{
  TRACE_FUNC
  if(isThreadRunning())
  {
    rtLogDebug("thread already started");
    return;
  }
  m_thread.reset(new std::thread(rtRemoteNetThread::runThread, this));
}

void rtRemoteNetThread::stopThread()
{
  TRACE_FUNC
  if(!isThreadRunning())
  {
    rtLogDebug("thread not running");
    return;
  }
  m_thread->join();
  m_thread.reset(nullptr);
}

void rtRemoteNetThread::runThread(rtRemoteNetThread* self)
{
  TRACE_FUNC
  self->onThreadRun();
}


/*****************************************************************************************
 * 
 * rtRemoteSocketServer
 *
*****************************************************************************************/

void rtRemoteSocketServer::connectSocket(std::shared_ptr<rtRemoteSocket> socket, rtRemoteSocketServer* server)
{
  TRACE_FUNC
  m_sockets.push_back(socket);
  socket->setServer(server);

  std::shared_ptr<rtRemoteSocketServerListener> listener = m_listener.lock();
  if(listener)
  {
    listener->onConnect(socket);
  }
}

void rtRemoteSocketServer::disconnectSocket(std::shared_ptr<rtRemoteSocket> socket)
{
  TRACE_FUNC
  disconnectSocket(socket.get());
}

void rtRemoteSocketServer::disconnectSocket(rtRemoteSocket* socket)
{
  TRACE_FUNC
  auto it = std::find_if(m_sockets.begin(), m_sockets.end(), 
   [socket](std::shared_ptr<rtRemoteSocket> const& s) { return s.get() == socket; });

  if(it == m_sockets.end())
    return;

  std::shared_ptr<rtRemoteSocketServerListener> listener = m_listener.lock();
  if(listener)
  {
    listener->onDisconnect(*it);
  }

  m_sockets.erase(it);
}

/*****************************************************************************************
 * 
 * rtRemoteNetFactory
 *
*****************************************************************************************/

std::unique_ptr<rtRemoteNetFactory>& rtRemoteNetFactory::get(rtRemoteNetType type)
{
  TRACE_FUNC

  static std::unique_ptr<rtRemoteNetFactory> factoryCSocket(new rtRemoteCSocketFactory);
  static std::unique_ptr<rtRemoteNetFactory> factoryWSocket(new rtRemoteWebSocketFactory);

  if(type == rtRemoteNetTypeWebSocket)
  {
    return factoryWSocket;
  }
  else if(type == rtRemoteNetTypeDefault)
  {
    //TODO use rtRemoteConfig to determine type
    return factoryCSocket;
  }
  else
  {
    return factoryCSocket;//default
  }
}

std::unique_ptr<rtRemoteNetFactory>& rtRemoteNetFactory::get(std::shared_ptr<rtRemoteAddress> addressHint)
{
  if(addressHint && addressHint->getScheme().compare("ws") == 0)//TODO -- test this line
  {
    return rtRemoteNetFactory::get(rtRemoteNetTypeWebSocket);
  }
  else
  {
    return rtRemoteNetFactory::get(rtRemoteNetTypeDefault);
  }
}
