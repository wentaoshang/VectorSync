/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_LOGGING_HPP_
#define NDN_VSYNC_LOGGING_HPP_

#ifdef NS3_LOG_ENABLE

#include "ns3/log.h"

#define VSYNC_LOG_DEFINE(name) NS_LOG_COMPONENT_DEFINE(#name)

#define VSYNC_LOG_TRACE(expr) NS_LOG_LOGIC(expr)
#define VSYNC_LOG_INFO(expr) NS_LOG_INFO(expr)
#define VSYNC_LOG_DEBUG(expr) NS_LOG_DEBUG(expr)
#define VSYNC_LOG_WARN(expr) NS_LOG_WARN(expr)
#define VSYNC_LOG_ERROR(expr) NS_LOG_ERROR(expr)

#else

#include <ndn-cxx/util/logger.hpp>

#define VSYNC_LOG_DEFINE(name) NDN_LOG_INIT(name)

#define VSYNC_LOG_TRACE(expr) NDN_LOG_TRACE(expr)
#define VSYNC_LOG_INFO(expr) NDN_LOG_INFO(expr)
#define VSYNC_LOG_DEBUG(expr) NDN_LOG_DEBUG(expr)
#define VSYNC_LOG_WARN(expr) NDN_LOG_WARN(expr)
#define VSYNC_LOG_ERROR(expr) NDN_LOG_ERROR(expr)

#endif

#endif  // NDN_VSYNC_LOGGING_HPP_
