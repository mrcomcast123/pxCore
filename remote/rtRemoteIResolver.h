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

#ifndef __RT_REMOTE_OBJECT_RESOLVER_H__
#define __RT_REMOTE_OBJECT_RESOLVER_H__

#include <memory>
#include <string>
#include <rtError.h>
#include <sys/socket.h>
#include <stdint.h>

#include "rtRemoteTypes.h"
#include "rtRemoteNet.h"

class rtRemoteIResolver
{
public:
  virtual ~rtRemoteIResolver() { }
  virtual rtError open(std::shared_ptr<rtRemoteAddress> const& rpc_csocket_endpoint, 
                       std::shared_ptr<rtRemoteAddress> const& rpc_websocket_endpoint = nullptr) = 0;
  virtual rtError close() = 0;
  virtual rtError registerObject(std::string const& name) = 0;
  virtual rtError locateObject(std::string const& name, 
                               std::shared_ptr<rtRemoteAddress>& endpoint, 
                               uint32_t timeout) = 0;
  virtual rtError unregisterObject(std::string const& name) = 0;
};

#endif
