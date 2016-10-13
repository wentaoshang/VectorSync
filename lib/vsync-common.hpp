/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_COMMON_HPP_
#define NDN_VSYNC_COMMON_HPP_

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace ndn {
namespace vsync {

// Type and constant declarations for VectorSync

using NodeID = std::string;
using ViewID = std::pair<uint64_t, NodeID>;
using VersionVector = std::vector<uint8_t>;
using NodeIndex = std::size_t;

// Extended sequence number
struct ESN {
  ViewID vi;
  uint64_t rn;
  uint64_t seq;
};

struct ESNCompare {
  bool operator() (const ESN& l, const ESN& r) {
    auto tl = std::make_tuple(l.vi.first, l.rn, l.seq);
    auto tr = std::make_tuple(r.vi.first, r.rn, r.seq);
    return tl < tr;
  }
};

static const Name kVsyncPrefix = Name("/ndn/broadcast/vsync");

static constexpr time::seconds kHeartbeatInterval = time::seconds(4);
static constexpr time::seconds kHeartbeatTimeout = 3 * kHeartbeatInterval;
static constexpr time::seconds kHealthcheckInterval = kHeartbeatInterval;
// Leader election timeout MUST be smaller than healthcheck interval
static constexpr time::seconds kLeaderElectionTimoutMax = time::seconds(3);

// Leader will only perform view change when the number of dead members exceeds
// kViewChangeThreadhold.
static const size_t kViewChangeThreshold = 1;

} // namespace vsync
} // namespace ndn

#endif // NDN_VSYNC_COMMON_HPP_
