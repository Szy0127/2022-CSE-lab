// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client()
{
  es = new extent_server();
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id,chfs_command::txid_t txid)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = es->create(type, id,txid);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = es->get(eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = es->getattr(eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf,chfs_command::txid_t txid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = es->put(eid, buf, r,txid);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid,chfs_command::txid_t txid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = es->remove(eid, r,txid);
  return ret;
}


extent_protocol::status
extent_client::begin(chfs_command::txid_t &txid)
{
  extent_protocol::status ret = extent_protocol::OK;
  txid = es->begin();
  return ret;
}

extent_protocol::status
extent_client::commit(chfs_command::txid_t txid)
{
  extent_protocol::status ret = extent_protocol::OK;
  es->commit(txid);
  return ret;
}

