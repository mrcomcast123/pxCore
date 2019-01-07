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

#ifndef __RT_REMOTE_CLIENT_H__
#define __RT_REMOTE_CLIENT_H__

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <rtError.h>
#include <rtValue.h>

#include <sys/socket.h>

#include "rtRemoteCorrelationKey.h"
#include "rtRemoteEnvironment.h"
#include "rtRemoteMessage.h"
#include "rtRemoteSocketUtils.h"
#include "rtRemoteNet.h"
#include "rtRemoteAsyncHandle.h"

class rtRemoteClient
  : public std::enable_shared_from_this<rtRemoteClient>
  , public rtRemoteSocketListener
{
public:
  enum class State
  {
    Started,
    Shutdown
  };

  using StateChangedHandler = rtError (*)(std::shared_ptr<rtRemoteClient> const& client,
    State state, void* argp);

  rtRemoteClient(rtRemoteEnvironment* env);
  ~rtRemoteClient();

  rtError connect(std::shared_ptr<rtRemoteAddress> const& remoteEndpoint);
  rtError accept(std::shared_ptr<rtRemoteSocket> const& socket);

  rtError startSession(std::string const& objectId, uint32_t timeout = 0);

  rtError sendSet(std::string const& objectId, uint32_t    propertyIdx , rtValue const& value);
  rtError sendSet(std::string const& objectId, char const* propertyName, rtValue const& value);
  rtError sendGet(std::string const& objectId, uint32_t    propertyIdx,  rtValue& result);
  rtError sendGet(std::string const& objectId, char const* propertyName, rtValue& result);
  rtError sendCall(std::string const& objectId, std::string const& methodName,
    int argc, rtValue const* argv, rtValue& result);

  void registerKeepAliveForObject(std::string const& s);
  rtError setStateChangedHandler(StateChangedHandler handler, void* argp);

  void removeKeepAliveForObject(std::string const& s);

  inline rtRemoteEnvironment* getEnvironment() const
    { return m_env; }

  rtError send(rtRemoteMessagePtr const& msg);

  std::shared_ptr<rtRemoteAddress> getEndpoint() const;

private:
  rtError sendGet(rtRemoteMessagePtr const& req, rtRemoteCorrelationKey k, rtValue& value);
  rtError sendSet(rtRemoteMessagePtr const& req, rtRemoteCorrelationKey k);
  rtError sendCall(rtRemoteMessagePtr const& req, rtRemoteCorrelationKey k, rtValue& result); 
  rtRemoteAsyncHandle sendWithWait(rtRemoteMessagePtr const& req, rtRemoteCorrelationKey k);

  // from rtRemoteSocketListener
  virtual rtError onMessage(std::shared_ptr<rtRemoteSocket> const& socket, rtRemoteMessagePtr const& doc) override;
  virtual rtError onStateChanged(std::shared_ptr<rtRemoteSocket> const& socket, rtRemoteSocketState state) override;

  rtError sendKeepAlive();

  static rtError onSynchronousResponse_Handler(std::shared_ptr<rtRemoteClient>& client,
        rtRemoteMessagePtr const& msg, void* argp)
    { return reinterpret_cast<rtRemoteClient *>(argp)->onSynchronousResponse(client, msg); }

  rtError onStartSession(rtRemoteMessagePtr const& doc);
  rtError waitForResponse(rtRemoteCorrelationKey k, rtRemoteMessagePtr& res, int timeout);
  rtError onSynchronousResponse(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& doc);
  // rtError sendSynchronousRequest(rtRemoteMessagePtr const& req, rtRemoteMessagePtr& res, int timeout);
  // bool moreToProcess(rtRemoteCorrelationKey k);

private:
  inline std::shared_ptr<rtRemoteSocket> getSocket()
  {
    std::shared_ptr<rtRemoteSocket> s;
    std::unique_lock<std::recursive_mutex> lock(m_mutex);
    if (m_socket)
      s = m_socket;
    return s;
  }
  rtError checkStream();

  std::shared_ptr<rtRemoteSocket>           m_socket;
  std::shared_ptr<rtRemoteAddress>          m_remoteEndpoint;
  std::vector<std::string>                  m_objects;
  std::recursive_mutex mutable              m_mutex;
  rtRemoteEnvironment*                      m_env;
  rtRemoteCallback<StateChangedHandler>     m_state_changed_handler;
};

#endif
