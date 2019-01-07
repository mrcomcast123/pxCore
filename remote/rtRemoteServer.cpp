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

#include "rtRemoteServer.h"
#include "rtRemoteCorrelationKey.h"
#include "rtRemoteEnvironment.h"
#include "rtRemoteFunction.h"
#include "rtRemoteObject.h"
#include "rtRemoteObjectCache.h"
#include "rtRemoteMessage.h"
#include "rtRemoteClient.h"
#include "rtRemoteValueReader.h"
#include "rtRemoteValueWriter.h"
#include "rtRemoteConfig.h"
#include "rtRemoteFactory.h"

#include <limits>
#include <sstream>
#include <set>
#include <algorithm>
#include <memory>

//#include <sys/stat.h>
#include <stdlib.h>
//#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
//#include <unistd.h>
//#include <fcntl.h>
#include <rtLog.h>
//#include <dirent.h>

rtRemoteServer::rtRemoteServer(rtRemoteEnvironment* env)
  : m_resolver(nullptr)
  , m_keep_alive_interval(std::numeric_limits<uint32_t>::max())
  , m_env(env)
{
  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeOpenSessionRequest, 
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onOpenSession_Dispatch, this)));

  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeGetByNameRequest,
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onGet_Dispatch, this)));

  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeGetByIndexRequest,
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onGet_Dispatch, this)));

  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeSetByNameRequest,
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onSet_Dispatch, this)));

  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeSetByIndexRequest,
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onSet_Dispatch, this)));

  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeMethodCallRequest,
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onMethodCall_Dispatch, this)));

  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeKeepAliveRequest,
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onKeepAlive_Dispatch, this)));

  m_command_handlers.insert(CommandHandlerMap::value_type(kMessageTypeKeepAliveResponse,
    rtRemoteCallback<rtRemoteMessageHandler>(&rtRemoteServer::onKeepAliveResponse_Dispatch, this)));
}

rtRemoteServer::~rtRemoteServer()
{
  if (m_resolver)
  {
    m_resolver->close();
    delete m_resolver;
  }
}

rtError
rtRemoteServer::open()
{
  rtError err;

  //start C Socket server
  m_csServer = rtRemoteNetFactory::get(rtRemoteNetTypeCSocket)->createServer();
  err = m_csServer->start(shared_from_this(), 0, nullptr);
  if(err != RT_OK)
  {
    rtLogError("failed to start rpc csocket server. %s", rtStrError(err));
    return err;
  }

  //start Web Socket server
  m_wsServer = rtRemoteNetFactory::get(rtRemoteNetTypeWebSocket)->createServer();
  err = m_wsServer->start(shared_from_this(), 0, nullptr);
  if(err != RT_OK)
  {
    rtLogError("failed to start rpc websocket server. %s", rtStrError(err));
    //don't return err here because we can run without web socket server
  }

  //start Resolver
  m_resolver = rtRemoteFactory::rtRemoteCreateResolver(m_env);
  err = m_resolver->open(m_csServer->getAddress(), m_wsServer ? m_wsServer->getAddress() : nullptr);
  if (err != RT_OK)
  {
    rtLogWarn("failed to open resolver. %s", rtStrError(err));
    return err;
  }

  return RT_OK;
}

rtError
rtRemoteServer::registerObject(std::string const& objectId, rtObjectRef const& obj)
{
  rtObjectRef ref = m_env->ObjectCache->findObject(objectId);
  if (!ref)
  {
    m_env->ObjectCache->insert(objectId, obj);
    m_env->ObjectCache->markUnevictable(objectId, true);
  }
  m_resolver->registerObject(objectId);
  return RT_OK;
}

rtError
rtRemoteServer::unregisterObject(std::string const& objectId)
{
  rtError e = RT_OK;

  if (m_env)
  {
    e = m_env->ObjectCache->markUnevictable(objectId, false);
    if (e != RT_OK)
    {
      rtLogInfo("failed to mark object %s for removal. %s",
          objectId.c_str(), rtStrError(e));
    }
  }

  if (m_resolver)
  {
    e = m_resolver->unregisterObject(objectId);
    if (e != RT_OK)
    {
      rtLogInfo("failed to remove resolver entry for %s. %s", objectId.c_str(),
          rtStrError(e));
    }
  }

  return e;
}

void
rtRemoteServer::onConnect(std::shared_ptr<rtRemoteSocket> socket)
{
  std::shared_ptr<rtRemoteClient> newClient(new rtRemoteClient(m_env));
  newClient->setStateChangedHandler(&rtRemoteServer::onClientStateChanged_Dispatch, this);
  newClient->accept(socket);
  m_connected_clients.push_back(newClient);
}

void
rtRemoteServer::onDisconnect(std::shared_ptr<rtRemoteSocket> socket)
{
  //TODO Do we need to do anything here because onClientStateChanged handles disconnect too
}

rtError
rtRemoteServer::onClientStateChanged(std::shared_ptr<rtRemoteClient> const& client,
  rtRemoteClient::State state)
{
  rtError e = RT_ERROR_OBJECT_NOT_FOUND;

  if (state == rtRemoteClient::State::Shutdown)
  {
    rtLogInfo("client shutdown");
    std::unique_lock<std::mutex> lock(m_mutex);
    auto itr = std::remove_if(
      m_connected_clients.begin(),
      m_connected_clients.end(),
      [&client](std::shared_ptr<rtRemoteClient> const& c) { return c.get() == client.get(); }
    );
    if (itr != m_connected_clients.end())
    {
      rtLogInfo("removing reference to stream");
      m_connected_clients.erase(itr);
      e = RT_OK;
    }

    auto ditr = m_disconnected_callback_map.find(client.get());
    if (ditr != m_disconnected_callback_map.end())
    {
        std::vector<ClientDisconnectedCB>::const_iterator cbitr;
        for(cbitr = ditr->second.cbegin(); cbitr != ditr->second.cend(); ++cbitr)
        {
            if(cbitr->func)
                cbitr->func(cbitr->data);
        }
        ditr->second.clear();
        m_disconnected_callback_map.erase(ditr);
    }
  }

  return e;
}

rtError
rtRemoteServer::onIncomingMessage(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& msg)
{
  rtLogInfo("onIncomingMessage");

  rtError e = RT_OK;
  if (m_env->Config->server_use_dispatch_thread())
    m_env->enqueueWorkItem(client, msg);
  else
    e = processMessage(client, msg);
  return e;
}

rtError
rtRemoteServer::processMessage(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& msg)
{
  rtError e = RT_FAIL;
  char const* msgType = rtMessage_GetMessageType(*msg);

  auto itr = m_command_handlers.find(msgType);
  if (itr == m_command_handlers.end())
  {
    rtLogError("processMessage: no command handler for:%s. RT_ERROR_PROTOCOL_ERROR", msgType);
    return RT_ERROR_PROTOCOL_ERROR;
  }

  rtRemoteCallback<rtRemoteMessageHandler> handler = itr->second;
  if (handler.Func != nullptr)
    e = handler.Func(client, msg, handler.Arg);
  else
    e = RT_ERROR_INVALID_ARG;

  if (e != RT_OK)
    rtLogWarn("failed to run command for %s. %s", msgType, rtStrError(e));

  return e;
}

rtError
rtRemoteServer::findObject(std::string const& objectId, rtObjectRef& obj, uint32_t timeout,
        clientDisconnectedCallback cb, void *cbdata)
{
  rtError err = RT_OK;
  obj = m_env->ObjectCache->findObject(objectId);

  // if object is not registered with us locally, then check network
  if (!obj)
  {
    std::shared_ptr<rtRemoteAddress> objectEndpoint;
    err = m_resolver->locateObject(objectId, objectEndpoint, timeout);

    rtLogDebug("object %s found at endpoint: %s", objectId.c_str(),
    objectEndpoint->toString().c_str());

    if (err == RT_OK)
    {
      std::shared_ptr<rtRemoteClient> client;
      std::string const endpointName = objectEndpoint->toString();

      std::unique_lock<std::mutex> lock(m_mutex);
      auto itr = m_object_map.find(endpointName);
      if (itr != m_object_map.end())
        client = itr->second;

      // if a client is already "hosting" this object, 
      // then just re-use it
      for (auto i : m_object_map)
      {
        //TODO WMR look closely at this next line and make sure its comparing sock_addr correctly
        if (rtRemoteAddress::compare(i.second->getEndpoint().get(), objectEndpoint.get()))
        {
          client = i.second;
          break;
        }
      }
      m_mutex.unlock();

      if (!client)
      {
        client.reset(new rtRemoteClient(m_env));
        client->setStateChangedHandler(&rtRemoteServer::onClientStateChanged_Dispatch, this);
        err = client->connect(objectEndpoint);
        if (err != RT_OK)
        {
          rtLogWarn("failed to start new client. %s", rtStrError(err));
          return err;
        }

        // we have race condition here. if the transport doesn't exist, two threads may
        // create one but only one will get inserted into the m_connected_client map. I'm not
        // sure that this really matters much
        std::unique_lock<std::mutex> lock(m_mutex);
        m_object_map.insert(ClientMap::value_type(endpointName, client));
      }

      if (client)
      {
        rtRemoteObject* remote(new rtRemoteObject(objectId, client));
        err = client->startSession(objectId);
        if (err == RT_OK)
        {
          obj = remote;
        }
        else
        {
          rtLogError("startSesssion failed");
        }

        ClientDisconnectedCB CB = {cb, cbdata};
        auto ditr = m_disconnected_callback_map.find(client.get());
        if (ditr == m_disconnected_callback_map.end())
        {
            std::vector<ClientDisconnectedCB> new_cb_vector;
            new_cb_vector.push_back(CB);
            m_disconnected_callback_map.insert(ClientDisconnectedCBMap::value_type(client.get(), new_cb_vector));
        }
        else
        {
            auto cbitr = std::find_if(ditr->second.begin(), ditr->second.end(),
                    [CB](const ClientDisconnectedCB &cb) { return cb == CB; });

            if(cbitr == ditr->second.end())
                ditr->second.push_back(CB);
        }
      }
    }
  }

  return (obj ? RT_OK : RT_FAIL);
}

rtError
rtRemoteServer::unregisterDisconnectedCallback( clientDisconnectedCallback cb, void *cbdata )
{
    ClientDisconnectedCB CB = {cb, cbdata};

    auto findClient = [&]() -> ClientDisconnectedCBMap::iterator {
        for (auto &&client = m_disconnected_callback_map.begin();
             client != m_disconnected_callback_map.end();
             ++client) {
            const std::vector<ClientDisconnectedCB> &installed_callbacks = client->second;
            auto &&callback = find_if(installed_callbacks.cbegin(), installed_callbacks.cend(), [&](const ClientDisconnectedCB& c) {
                    return c == CB;
                });

            if (callback != installed_callbacks.end())
                return client;
        }
        return m_disconnected_callback_map.end();
    };

    std::unique_lock<std::mutex> lock(m_mutex);
    auto client = findClient();

    if (client == m_disconnected_callback_map.end()) {
        rtLogInfo("%p : %p client not found", cb, cbdata);
        return RT_ERROR_INVALID_ARG;
    }

    std::vector<ClientDisconnectedCB> &allClientCallbacks = client->second;
    allClientCallbacks.erase(
        std::remove_if(allClientCallbacks.begin(),
                       allClientCallbacks.end(),
                       [&] (const ClientDisconnectedCB& item) { return item == CB; }),
        allClientCallbacks.end()
        );
    rtLogInfo("%p : %p removed callback pair from the callbacks list", cb, cbdata);

    return RT_OK;
}

rtError
rtRemoteServer::onOpenSession(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& req)
{
  rtRemoteCorrelationKey key = rtMessage_GetCorrelationKey(*req);
  char const* objectId = rtMessage_GetObjectId(*req);

  #if 0
  std::unique_lock<std::mutex> lock(m_mutex);
  auto itr = m_objects.find(id);
  if (itr != m_objects.end())
  {
    sockaddr_storage const soc = client->getRemoteEndpoint();
    for (auto const& c : m_clients)
    {
      if (sameEndpoint(soc, c.peer))
      {
        rtLogInfo("new session for %s added to %s", rtSocketToString(soc).c_str(), id);
        itr->second.client_fds.push_back(c.fd);
        fd = c.fd;
        break;
      }
    }
  }
  lock.unlock();
  #endif

  rtError err = RT_OK;

  rtRemoteMessagePtr res(new rapidjson::Document());
  res->SetObject();
  res->AddMember(kFieldNameMessageType, kMessageTypeOpenSessionResponse, res->GetAllocator());
  res->AddMember(kFieldNameObjectId, std::string(objectId), res->GetAllocator());
  res->AddMember(kFieldNameCorrelationKey, key.toString(), res->GetAllocator());
  err = client->send(res);

  return err;
}

rtError
rtRemoteServer::onGet(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& doc)
{
  rtRemoteCorrelationKey key = rtMessage_GetCorrelationKey(*doc);
  char const* objectId = rtMessage_GetObjectId(*doc);

  rtRemoteMessagePtr res(new rapidjson::Document());
  res->SetObject();
  res->AddMember(kFieldNameMessageType, kMessageTypeGetByNameResponse, res->GetAllocator());
  res->AddMember(kFieldNameCorrelationKey, key.toString(), res->GetAllocator());
  res->AddMember(kFieldNameObjectId, std::string(objectId), res->GetAllocator());

  rtObjectRef obj = m_env->ObjectCache->findObject(objectId);
  if (!obj)
  {
    res->AddMember(kFieldNameStatusCode, 1, res->GetAllocator());
    res->AddMember(kFieldNameStatusMessage, std::string("object not found"), res->GetAllocator());
  }
  else
  {
    rtError err = RT_OK;
    rtValue value;

    uint32_t    index = 0;
    char const* name = rtMessage_GetPropertyName(*doc);

    if (name)
    {
      err = obj->Get(name, &value);
      if (err != RT_OK)
      {
        rtLogWarn("failed to get property: %s. %s", name, rtStrError(err));
      }
    }
    else
    {
      index = rtMessage_GetPropertyIndex(*doc);
      if (index != kInvalidPropertyIndex)
        err = obj->Get(index, &value);
    }
    if (err == RT_OK)
    {
      rapidjson::Value val;
      if (value.getType() == RT_functionType)
      {
        val.SetObject();
        val.AddMember(kFieldNameObjectId, std::string(objectId), res->GetAllocator());
        if (name)
        {
          rtFunctionRef ref = value.toFunction();
          rtRemoteFunction* remoteFunc = dynamic_cast<rtRemoteFunction *>(ref.getPtr());
          if (remoteFunc != nullptr)
            val.AddMember(kFieldNameFunctionName, remoteFunc->getName(), res->GetAllocator());
          else
            val.AddMember(kFieldNameFunctionName, std::string(name), res->GetAllocator());
        }
        else
        {
          val.AddMember(kFieldNameFunctionIndex, index, res->GetAllocator());
        }
        val.AddMember(kFieldNameValueType, static_cast<int>(RT_functionType), res->GetAllocator());
      }
      else
      {
        err = rtRemoteValueWriter::write(m_env, value, val, *res);
        if (err != RT_OK)
          rtLogWarn("failed to write value: %d", err);
      }
      res->AddMember(kFieldNameValue, val, res->GetAllocator());
      res->AddMember(kFieldNameStatusCode, 0, res->GetAllocator());
    }
    else
    {
      res->AddMember(kFieldNameStatusCode, static_cast<int32_t>(err), res->GetAllocator());
    }

    err = client->send(res);
  }

  return RT_OK;
}

rtError
rtRemoteServer::onSet(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& doc)
{
  rtRemoteCorrelationKey key = rtMessage_GetCorrelationKey(*doc);
  char const* objectId = rtMessage_GetObjectId(*doc);

  rtRemoteMessagePtr res(new rapidjson::Document());
  res->SetObject();
  res->AddMember(kFieldNameMessageType, kMessageTypeSetByNameResponse, res->GetAllocator());
  res->AddMember(kFieldNameCorrelationKey, key.toString(), res->GetAllocator());
  res->AddMember(kFieldNameObjectId, std::string(objectId), res->GetAllocator());

  rtObjectRef obj = m_env->ObjectCache->findObject(objectId);
  if (!obj)
  {
    res->AddMember(kFieldNameStatusCode, 1, res->GetAllocator());
    res->AddMember(kFieldNameStatusMessage, std::string("object not found"), res->GetAllocator());
  }
  else
  {
    uint32_t index;
    rtError err = RT_FAIL;

    rtValue value;

    auto itr = doc->FindMember(kFieldNameValue);
    RT_ASSERT(itr != doc->MemberEnd());

    if (itr != doc->MemberEnd())
      err = rtRemoteValueReader::read(m_env, value, itr->value, client);

    if (err == RT_OK)
    {
      char const* name = rtMessage_GetPropertyName(*doc);

      if (name)
      {
        err = obj->Set(name, &value);
      }
      else
      {
        index = rtMessage_GetPropertyIndex(*doc);
        if (index != kInvalidPropertyIndex)
          err = obj->Set(index, &value);
      }
    }

    res->AddMember(kFieldNameStatusCode, static_cast<int>(err), res->GetAllocator());
    err = client->send(res);
  }
  return RT_OK;
}

rtError
rtRemoteServer::onMethodCall(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& doc)
{
  rtRemoteCorrelationKey key = rtMessage_GetCorrelationKey(*doc);
  char const* objectId = rtMessage_GetObjectId(*doc);
  rtError err   = RT_OK;

  rtRemoteMessagePtr res(new rapidjson::Document());
  res->SetObject();
  res->AddMember(kFieldNameMessageType, kMessageTypeMethodCallResponse, res->GetAllocator());
  res->AddMember(kFieldNameCorrelationKey, key.toString(), res->GetAllocator());

  rtObjectRef obj = m_env->ObjectCache->findObject(objectId);
  if (!obj && (strcmp(objectId, "global") != 0))
  {
    rtMessage_SetStatus(*res, 1, "failed to find object with id: %s", objectId);
  }
  else
  {
    auto function_name = doc->FindMember(kFieldNameFunctionName);
    if (function_name == doc->MemberEnd())
    {
      rtLogInfo("message missing %s field", kFieldNameFunctionName);
      return RT_FAIL;
    }

    rtFunctionRef func;
    if (obj) // member function
    {
      err = obj.get<rtFunctionRef>(function_name->value.GetString(), func);
    }
    else
    {
      func = m_env->ObjectCache->findFunction(function_name->value.GetString());
    }

    if (err == RT_OK && !!func)
    {
      // virtual rtError Send(int numArgs, const rtValue* args, rtValue* result) = 0;
      std::vector<rtValue> argv;

      auto itr = doc->FindMember(kFieldNameFunctionArgs);
      if (itr != doc->MemberEnd())
      {
        // rapidjson::Value const& args = (*doc)[kFieldNameFunctionArgs];
        rapidjson::Value const& args = itr->value;
        for (rapidjson::Value::ConstValueIterator itr = args.Begin(); itr != args.End(); ++itr)
        {
          rtValue arg;
          rtRemoteValueReader::read(m_env, arg, *itr, client);
          argv.push_back(arg);
        }
      }

      rtValue return_value;
      err = func->Send(static_cast<int>(argv.size()), &argv[0], &return_value);
      if (err == RT_OK)
      {
        rapidjson::Value val;
        rtRemoteValueWriter::write(m_env, return_value, val, *res);
        res->AddMember(kFieldNameFunctionReturn, val, res->GetAllocator());
      }

      rtMessage_SetStatus(*res, 0);
    }
    else
    {
      rtMessage_SetStatus(*res, 1, "object not found");
    }
  }

  err = client->send(res);
  if (err != RT_OK)
    rtLogWarn("failed to send response. %d", err);

  return RT_OK;
}

rtError
rtRemoteServer::onKeepAlive(std::shared_ptr<rtRemoteClient>& client, rtRemoteMessagePtr const& req)
{
  rtRemoteCorrelationKey key = rtMessage_GetCorrelationKey(*req);

  auto itr = req->FindMember(kFieldNameKeepAliveIds);
  if (itr != req->MemberEnd())
  {
    auto now = std::chrono::steady_clock::now();

    std::unique_lock<std::mutex> lock(m_mutex);
    for (rapidjson::Value::ConstValueIterator id  = itr->value.Begin(); id != itr->value.End(); ++id)
    {
      rtError e = m_env->ObjectCache->touch(id->GetString(), now);
      if (e != RT_OK)
      {
        rtLogWarn("error updating last used time for: %s, %s", 
            id->GetString(), rtStrError(e));
      }
    }
  }
  else
  {
    rtLogWarn("got keep-alive without any interesting information");
  }

  rtRemoteMessagePtr res(new rapidjson::Document());
  res->SetObject();
  res->AddMember(kFieldNameCorrelationKey, key.toString(), res->GetAllocator());
  res->AddMember(kFieldNameMessageType, kMessageTypeKeepAliveResponse, res->GetAllocator());
  return client->send(res);
}

rtError
rtRemoteServer::onKeepAliveResponse(std::shared_ptr<rtRemoteClient>& /*client*/, rtRemoteMessagePtr const& /*req*/)
{
    return RT_OK;
}

rtError
rtRemoteServer::removeStaleObjects()
{
  std::unique_lock<std::mutex> lock(m_mutex);
  for (auto itr = m_object_map.begin(); itr != m_object_map.end();)
  {
    // TODO: I'm not sure if this works. This is a map of connections to remote peers.
    // when connecting to mulitple remote objects, we'll re-use the remote connection.
    if (itr->second.use_count() == 1)
      itr = m_object_map.erase(itr);
    else
      ++itr;
  }
  lock.unlock();
  return m_env->ObjectCache->removeUnused(); // m_keep_alive_interval, num_removed);
}
