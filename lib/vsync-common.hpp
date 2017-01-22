/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_COMMON_HPP_
#define NDN_VSYNC_COMMON_HPP_

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <ndn-cxx/name.hpp>
#include <ndn-cxx/util/time.hpp>

#include "vsync-message.pb.h"

namespace ndn {
namespace vsync {

// Type and constant declarations for VectorSync

using NodeID = std::string;
using ViewID = std::pair<uint64_t, NodeID>;
using VersionVector = std::vector<uint64_t>;
using NodeIndex = std::size_t;

static const Name kSyncPrefix = Name("/ndn/broadcast/vsync");

static constexpr time::seconds kHeartbeatInterval = time::seconds(4);
static constexpr time::seconds kHeartbeatTimeout = 3 * kHeartbeatInterval;
static constexpr time::seconds kHealthcheckInterval = kHeartbeatInterval;
// Leader election timeout MUST be smaller than healthcheck interval
static constexpr time::seconds kLeaderElectionTimeoutMax = time::seconds(3);
static_assert(kLeaderElectionTimeoutMax < kHealthcheckInterval,
              "Leader election timeout must be less than healthcheck interval");

// Leader will only perform view change when the number of dead members exceeds
// kViewChangeThreadhold.
static const size_t kViewChangeThreshold = 1;

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_COMMON_HPP_
