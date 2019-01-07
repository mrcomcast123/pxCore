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

#include "rtRemoteNetCSocket.h"
#include "rtRemoteSocketUtils.h"
#include "rtRemoteEnvironment.h"
#include "rtRemoteConfig.h"
#include "rtRemote.h"
#include <queue>
#include <set>
#include <memory>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <condition_variable>
#include <algorithm>

extern rtRemoteEnvironment* gEnv;

namespace
{
  bool
  isValidPid(char const* s)
  {
    while (s && *s)
    {
      if (!isdigit(*s++))
        return false;
    }
    return true;
  } // isValidPid

  int
  parsePid(char const* s)
  {
    int pid = -1;

    char const* p = s + strlen(s) - 1;
    while (p && *p)
    {
      if (!isdigit(*p))
        break;
      p--;
    }
    if (p)
    {
      p++;
      pid = strtol(p, nullptr, 10);
    }
    return pid;
  } // parsePid

  void
  cleanupStaleUnixSockets()
  {
    DIR* d = opendir("/proc/");
    if (!d)
    {
      rtError e = rtErrorFromErrno(errno);
      rtLogWarn("failed to open directory /proc. %s", rtStrError(e));
      return;
    }

    dirent* entry = reinterpret_cast<dirent *>(malloc(1024));
    dirent* result = nullptr;

    std::set<int> active_pids;

    int ret = 0;
    do
    {
      ret = readdir_r(d, entry, &result);
      if (ret == 0 && (result != nullptr))
      {
        if (isValidPid(result->d_name))
        {
          int pid = static_cast<int>(strtol(result->d_name, nullptr, 10));
          active_pids.insert(pid);
        }
      }
    }
    while ((result != nullptr) && ret == 0);
    closedir(d);

    d = opendir("/tmp");
    if (!d)
    {
      rtError e = rtErrorFromErrno(errno);
      rtLogWarn("failed to open directory /tmp. %s", rtStrError(e));
      return;
    }

    char path[UNIX_PATH_MAX];
    do
    {
      ret = readdir_r(d, entry, &result);
      if (ret == 0 && (result != nullptr))
      {
        memset(path, 0, sizeof(path));
        strcpy(path, "/tmp/");
        strcat(path, result->d_name);
        if (strncmp(path, kUnixSocketTemplateRoot, strlen(kUnixSocketTemplateRoot)) == 0)
        {
          int pid = parsePid(result->d_name);
          if (active_pids.find(pid) == active_pids.end())
          {
            rtLogDebug("removing inactive unix socket %s", path);
            int ret = unlink(path);
            if (ret == -1)
            {
              rtError e = rtErrorFromErrno(errno);
              rtLogWarn("failed to remove inactive unix socket %s. %s",
                path, rtStrError(e));
            }
          }
        }
      }
    }
    while ((result != nullptr) && ret == 0);

    closedir(d);
    free(entry);
  } // cleanupStaleUnixSockets

  bool
  isUnixDomain(rtRemoteEnvironment* env)
  {
    if (!env)
      return false;
  
    std::string family = env->Config->server_socket_family();
    if (!strcmp(family.c_str(), "unix"))
      return true;

    return false;
  } // isUnixDomain

  bool
  sameEndpoint(sockaddr_storage const& addr1, sockaddr_storage const& addr2)
  {
    if (addr1.ss_family != addr2.ss_family)
      return false;

    if (addr1.ss_family == AF_INET)
    {
      sockaddr_in const* in1 = reinterpret_cast<sockaddr_in const*>(&addr1);
      sockaddr_in const* in2 = reinterpret_cast<sockaddr_in const*>(&addr2);

      if (in1->sin_port != in2->sin_port)
        return false;

      return in1->sin_addr.s_addr == in2->sin_addr.s_addr;
    }

    if (addr1.ss_family == AF_UNIX)
    {
      sockaddr_un const* un1 = reinterpret_cast<sockaddr_un const*>(&addr1);
      sockaddr_un const* un2 = reinterpret_cast<sockaddr_un const*>(&addr2);

      return 0 == strncmp(un1->sun_path, un2->sun_path, UNIX_PATH_MAX);
    }

    RT_ASSERT(false);
    return false;
  } // sameEndpoint
} // namespace


/*****************************************************************************************
 * 
 * rtRemoteCSocketClient : Handles polling file descriptors on all open rtRemoteCSocket instances
 * 
 ****************************************************************************************/

class rtRemoteCSocketClient : public rtRemoteNetThread
{
public:
  static rtRemoteCSocketClient* instance()
  {
    static rtRemoteCSocketClient* pinstance = nullptr;
    if(!pinstance)
    {
      pinstance = new rtRemoteCSocketClient();
      pinstance->start();
    }
    return pinstance;
  }
  rtError start();
  rtError stop();
protected:
  void addSocket(std::shared_ptr<rtRemoteCSocket> s);
  void removeSocket(std::shared_ptr<rtRemoteCSocket> s);
  virtual void onThreadRun();
private:
  std::vector< std::shared_ptr<rtRemoteCSocket> > m_streams;
  pthread_t m_thread;
  std::mutex m_mutex;
  std::condition_variable m_streams_cond;
  int m_shutdown_pipe[2];
  bool m_running;
  friend class rtRemoteCSocket;
};

rtError rtRemoteCSocketClient::start()
{
  TRACE_FUNC
  if(isThreadRunning())
  {
    return RT_ERROR;
  }
  m_running = true;
  startThread();

  return RT_OK;
}

rtError rtRemoteCSocketClient::stop()
{
  m_running = false;
  m_streams_cond.notify_all();
}

void rtRemoteCSocketClient::addSocket(std::shared_ptr<rtRemoteCSocket> s)
{
  TRACE_FUNC
  m_streams.push_back(s);
  m_streams_cond.notify_all();
}

void rtRemoteCSocketClient::removeSocket(std::shared_ptr<rtRemoteCSocket> s)
{
  TRACE_FUNC
  for(std::vector< std::shared_ptr<rtRemoteCSocket> >::iterator it = m_streams.begin(); it != m_streams.end(); ++it)
  {
    if( (*it) == s )
    {
      m_streams.erase(it);
      return;
    }
  }
}

void rtRemoteCSocketClient::onThreadRun()
{
  TRACE_FUNC

  rtRemoteSocketBuffer buff;
  buff.reserve(gEnv->Config->stream_socket_buffer_size());

  const auto keepAliveInterval = std::chrono::seconds(gEnv->Config->stream_keep_alive_interval());
  auto lastKeepAliveSent = std::chrono::steady_clock::now();

  while (true)
  {
    int maxFd = 0;

    fd_set readFds;
    fd_set errFds;

    FD_ZERO(&readFds);
    FD_ZERO(&errFds);

    {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_streams_cond.wait(lock, [&]() {return !m_streams.empty() || !m_running;});
    if (!m_running)
      return;

    // remove dead streams
    {
      auto itr = std::remove_if(m_streams.begin(), m_streams.end(),
          [](std::shared_ptr<rtRemoteCSocket> const& s)
          {
            return !s->isConnected();
          });

      if (itr != m_streams.end())
        m_streams.erase(itr);
    }

    for (auto const& s : m_streams)
    {
      rtPushFd(&readFds, s->m_fd, &maxFd);
      rtPushFd(&errFds, s->m_fd, &maxFd);
    }
    }
#ifdef USE_SHUTDOWN_PIPE
    rtPushFd(&readFds, m_shutdown_pipe[0], &maxFd);
#endif
    timeval timeout;
    timeout.tv_sec = gEnv->Config->stream_select_interval();
    timeout.tv_usec = 0;

    int ret = select(maxFd + 1, &readFds, NULL, &errFds, &timeout);
    if (ret == -1)
    {
      rtError e = rtErrorFromErrno(errno);
      rtLogWarn("select failed: %s", rtStrError(e));
      continue;
    }

#ifdef USE_SHUTDOWN_PIPE
    if (FD_ISSET(m_shutdown_pipe[0], &readFds))
    {
      rtLogInfo("got shutdown signal");
      return;
    }
#endif
    auto now = std::chrono::steady_clock::now();
    bool sentKeepAlive = false;

    std::vector<std::shared_ptr<rtRemoteCSocket> > cleanup;

    std::unique_lock<std::mutex> lock(m_mutex);
    for (int i = 0, n = static_cast<int>(m_streams.size()); i < n; ++i)
    {
      rtError e = RT_OK;
      std::shared_ptr<rtRemoteCSocket> s = m_streams[i];
      if (FD_ISSET(s->m_fd, &readFds))
      {
        rtRemoteMessagePtr doc = nullptr;
        rtError e = rtReadMessage(s->m_fd, buff, doc);
        if(e == RT_OK)
        {
          printf("client onMessage\n");
          e = s->onMessage(doc);
        }
        if (e != RT_OK)
        {
          rtLogWarn("error reading message: %s.  closing socket", rtStrError(e));
          cleanup.push_back(s);        
          continue;
        }
      }
      else if (FD_ISSET(s->m_fd, &errFds))
      {
        // TODO
        rtLogError("error on fd: %d", s->m_fd);
      }

      if (s && s->isConnected() && (now - lastKeepAliveSent) > keepAliveInterval)
      {
        sentKeepAlive = true;

        // This really isn't inactivity, it's more like a timer enve
        e = s->onInactivity();
        if (e != RT_OK)
          rtLogWarn("error sending keep alive. %s", rtStrError(e));
      }
    }

    if (sentKeepAlive)
    {
      lastKeepAliveSent = now;
    }

    for (size_t i = 0; i < cleanup.size(); ++i)
    {
      cleanup[i]->disconnect();
    }

    // remove all dead streams
    auto end = std::remove_if(m_streams.begin(), m_streams.end(),
        [](std::shared_ptr<rtRemoteCSocket> const& s) { return s == nullptr; });
    m_streams.erase(end, m_streams.end());
    // rtLogDebug("streams size:%d", (int) m_streams.size());
    lock.unlock();
  }

}


/*****************************************************************************************
 * 
 * rtRemoteCSocketServer
 *
 ****************************************************************************************/

rtRemoteCSocketServer::rtRemoteCSocketServer()
{
  TRACE_FUNC
#ifdef USE_SHUTDOWN_PIPE
  m_shutdown_pipe[0] = -1;
  m_shutdown_pipe[1] = -1;
#endif
}

rtRemoteCSocketServer::~rtRemoteCSocketServer()
{
  TRACE_FUNC
  stop();
}

rtError rtRemoteCSocketServer::start(std::shared_ptr<rtRemoteSocketServerListener> listener, int port, const char* host)
{
  TRACE_FUNC

  rtLogWarn("CSocket Server Starting\n");

  if(isThreadRunning())
  {
    rtLogDebug("rtRemoteSocketServer already started.");
    return RT_ERROR;
  }
  m_listener = listener;

  if(port == 0)
  {
    //TODO - get from config
  }

  if(host == nullptr)
  {
    //TODO - get from config
  }

  ////////////////////////////////////////////
  //taken from rtRemoteSocketServer::openRpcListener

  int ret = 0;
  char path[UNIX_PATH_MAX];

  memset(path, 0, sizeof(path));
  cleanupStaleUnixSockets();

  sockaddr_storage m_rpc_endpoint;

  bool isUnix = isUnixDomain(gEnv);
  if (isUnix)
  {
    rtError e = rtCreateUnixSocketName(0, path, sizeof(path));
    if (e != RT_OK)
      return e;

    ret = unlink(path); // reuse path if needed
    if (ret == -1 && errno != ENOENT)
    {
      rtError e = rtErrorFromErrno(errno);
      rtLogError("error trying to remove %s. %s", path, rtStrError(e));
    }

    struct sockaddr_un *unAddr = reinterpret_cast<sockaddr_un*>(&m_rpc_endpoint);
    unAddr->sun_family = AF_UNIX;
    strncpy(unAddr->sun_path, path, UNIX_PATH_MAX);
  }
  else if (gEnv->Config->server_listen_interface() != "lo")
  {
    rtError e = rtParseAddress(m_rpc_endpoint, gEnv->Config->server_listen_interface().c_str(), 0, nullptr);
    if (e != RT_OK)
        rtGetDefaultInterface(m_rpc_endpoint, 0);
  }
  else
  {
    rtGetDefaultInterface(m_rpc_endpoint, 0);
  }

  m_listen_fd = socket(m_rpc_endpoint.ss_family, SOCK_STREAM, 0);
  if (m_listen_fd < 0)
  {
    rtError e = rtErrorFromErrno(errno);
    rtLogError("failed to create socket. %s", rtStrError(e));
    return e;
  }

  fcntl(m_listen_fd, F_SETFD, fcntl(m_listen_fd, F_GETFD) | FD_CLOEXEC);

  if (m_rpc_endpoint.ss_family != AF_UNIX)
  {
    uint32_t one = 1;
    if (-1 == setsockopt(m_listen_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)))
      rtLogError("setting TCP_NODELAY failed");
  }

  socklen_t len;
  rtSocketGetLength(m_rpc_endpoint, &len);

  ret = ::bind(m_listen_fd, reinterpret_cast<sockaddr *>(&m_rpc_endpoint), len);
  if (ret < 0)
  {
    rtError e = rtErrorFromErrno(errno);
    rtLogError("failed to bind socket. %s", rtStrError(e));
    return e;
  }

  rtGetSockName(m_listen_fd, m_rpc_endpoint);
  rtLogInfo("local rpc listener on: %s", rtSocketToString(m_rpc_endpoint).c_str());

  ret = fcntl(m_listen_fd, F_SETFL, O_NONBLOCK);
  if (ret < 0)
  {
    rtError e = rtErrorFromErrno(errno);
    rtLogError("fcntl: %s", rtStrError(e));
    return e;
  }

  ret = listen(m_listen_fd, 2);
  if (ret < 0)
  {
    rtError e = rtErrorFromErrno(errno);
    rtLogError("failed to put socket in listen mode. %s", rtStrError(e));
    return e;
  }

  ////////////////////////////////////////////

  if(isUnix)
    m_address = rtRemoteFileAddress::fromSockAddr(m_rpc_endpoint);
  else
    m_address = rtRemoteSocketAddress::fromSockAddr("tcp", m_rpc_endpoint);
#ifdef USE_SHUTDOWN_PIPE
  ret = pipe2(m_shutdown_pipe, O_CLOEXEC);
  if (ret != 0)
  {
    rtError e = rtErrorFromErrno(ret);
    rtLogWarn("failed to create shutdown pipe. %s", rtStrError(e));
  }
#endif
  startThread();
  return RT_OK;
}

rtError rtRemoteCSocketServer::stop()
{
  TRACE_FUNC

#ifdef USE_SHUTDOWN_PIPE
  if (m_shutdown_pipe[0] != -1)
  {
    char buff[] = {"shutdown"};
    ssize_t n = write(m_shutdown_pipe[1], buff, sizeof(buff));
    if (n == -1)
    {
      rtError e = rtErrorFromErrno(errno);
      rtLogWarn("failed to write. %s", rtStrError(e));
    }
  }
#endif
  stopThread();

  if (m_listen_fd != -1)
    ::close(m_listen_fd);
#ifdef USE_SHUTDOWN_PIPE
  if (m_shutdown_pipe[0] != -1)
    ::close(m_shutdown_pipe[0]);
  if (m_shutdown_pipe[1] != -1)
    ::close(m_shutdown_pipe[1] = -1);
#endif
}

void rtRemoteCSocketServer::onThreadRun()
{
  TRACE_FUNC

  while (true)
  {
    int maxFd = 0;

    fd_set readFds;
    fd_set errFds;

    FD_ZERO(&readFds);
    rtPushFd(&readFds, m_listen_fd, &maxFd);
#ifdef USE_SHUTDOWN_PIPE
    rtPushFd(&readFds, m_shutdown_pipe[0], &maxFd);
#endif

    FD_ZERO(&errFds);
    rtPushFd(&errFds, m_listen_fd, &maxFd);

    timeval timeout;
    timeout.tv_sec = 1; // TODO: move to rtremote.conf (rt.remote.server.select.timeout)
    timeout.tv_usec = 0;

    int ret = select(maxFd + 1, &readFds, NULL, &errFds, &timeout);
    if (ret == -1)
    {
      rtError e = rtErrorFromErrno(errno);
      rtLogWarn("select failed: %s", rtStrError(e));
      continue;
    }

#ifdef USE_SHUTDOWN_PIPE
    // right now we just use this to signal "hey" more fds added
    // later we'll use this to shutdown
    if (FD_ISSET(m_shutdown_pipe[0], &readFds))
    {
      rtLogInfo("got shutdown signal");
      return;
    }
#endif

    if (FD_ISSET(m_listen_fd, &readFds))
    {
      sockaddr_storage remoteEndpoint;
      memset(&remoteEndpoint, 0, sizeof(remoteEndpoint));

      socklen_t len = sizeof(sockaddr_storage);

      int fd = accept(m_listen_fd, reinterpret_cast<sockaddr *>(&remoteEndpoint), &len);
      if (fd == -1)
      {
        rtError e = rtErrorFromErrno(errno);
        rtLogWarn("error accepting new tcp connect. %s", rtStrError(e));
        return;
      }
      rtLogInfo("new connection from %s with fd:%d", rtSocketToString(remoteEndpoint).c_str(), fd);

      sockaddr_storage localEndpoint;
      memset(&localEndpoint, 0, sizeof(sockaddr_storage));
      rtGetSockName(m_listen_fd, localEndpoint);

      auto socket = std::make_shared<rtRemoteCSocket>();
      socket->accept(fd);
      connectSocket(socket, this);
    }
  }
}


/*****************************************************************************************
 * 
 * rtRemoteCSocket
 *
*****************************************************************************************/

rtRemoteCSocket::rtRemoteCSocket()
: rtRemoteSocket(),
  m_fd(-1)
{
  TRACE_FUNC
}

rtRemoteCSocket::~rtRemoteCSocket()
{
  TRACE_FUNC
}

rtError rtRemoteCSocket::connect(rtRemoteAddress* remoteAddress)
{
  TRACE_FUNC

  if(isConnected())
    disconnect();

  sockaddr_storage endpoint = remoteAddress->toSockAddr();

  m_fd = socket(endpoint.ss_family, SOCK_STREAM, 0);
  if (m_fd < 0)
  {
    rtError e = rtErrorFromErrno(errno);
    rtLogError("failed to create socket. %s", rtStrError(e));
    return RT_FAIL;
  }
  fcntl(m_fd, F_SETFD, fcntl(m_fd, F_GETFD) | FD_CLOEXEC);

  if (endpoint.ss_family != AF_UNIX)
  {
    uint32_t one = 1;
    if (-1 == setsockopt(m_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)))
      rtLogError("setting TCP_NODELAY failed\n");
  }

  socklen_t len;
  rtSocketGetLength(endpoint, &len);

  int ret = ::connect(m_fd, reinterpret_cast<sockaddr const *>(&endpoint), len);
  if (ret < 0)
  {
    rtError e = rtErrorFromErrno(errno);
    rtLogError("failed to connect to remote rpc endpoint. %s", rtStrError(e));
    rtCloseSocket(m_fd);
    return RT_FAIL;
  }
/*
  rtGetSockName(m_fd, m_local_endpoint);
  rtGetPeerName(m_fd, m_remote_endpoint);

  rtLogInfo("new connection (%d) %s --> %s",
    m_fd,
    rtSocketToString(m_local_endpoint).c_str(),
    rtSocketToString(m_remote_endpoint).c_str());
*/
  rtRemoteCSocketClient::instance()->addSocket(shared_from_this());
}

rtError rtRemoteCSocket::accept(int fd)
{
  m_fd = fd;
  rtRemoteCSocketClient::instance()->addSocket(shared_from_this());
}

rtError rtRemoteCSocket::disconnect()
{
  TRACE_FUNC
  onDisconnect();

  if (m_fd != kInvalidSocket)
  {
    rtRemoteCSocketClient::instance()->removeSocket(shared_from_this());
    // rtRemoteStreamSelector will remove dead streams on its own
    int ret = ::shutdown(m_fd, SHUT_RDWR);
    if (ret == -1)
    {
      rtError e = rtErrorFromErrno(errno);
      rtLogDebug("shutdown failed on fd %d: %s", m_fd, rtStrError(e));
    }

    // sets m_fd to kInvalidSocket
    rtCloseSocket(m_fd);
  }
  return RT_OK;
}

bool rtRemoteCSocket::isConnected()
{
  //TRACE_FUNC
  return m_fd != kInvalidSocket; 
}

rtError rtRemoteCSocket::send(const char *message, size_t length)
{
  //TRACE_FUNC
  return RT_OK;//rtSendBuffer(m_fd, message, (int)length);
}

rtError rtRemoteCSocket::send(rtRemoteMessagePtr const& msg)
{
  //TRACE_FUNC
  return rtSendDocument(*msg, m_fd, nullptr);
}


/*****************************************************************************************
 * 
 * rtRemoteCSocketFactory
 *
*****************************************************************************************/

std::shared_ptr<rtRemoteSocket> rtRemoteCSocketFactory::createSocket()
{
  TRACE_FUNC
  return std::static_pointer_cast<rtRemoteSocket>(std::make_shared<rtRemoteCSocket>());
}

std::shared_ptr<rtRemoteSocketServer> rtRemoteCSocketFactory::createServer()
{
  TRACE_FUNC
  return std::static_pointer_cast<rtRemoteSocketServer>(std::make_shared<rtRemoteCSocketServer>());
}
