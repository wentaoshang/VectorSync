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

struct ViewID {
  uint64_t view_num;
  Name leader_name;

  friend bool operator==(const ViewID& l, const ViewID& r) {
    return l.view_num == r.view_num && l.leader_name == r.leader_name;
  }

  friend bool operator!=(const ViewID& l, const ViewID& r) { return !(l == r); }
};

struct VIDCompare {
  bool operator()(const ViewID& l, const ViewID& r) const {
    return l.view_num < r.view_num ||
           (l.view_num == r.view_num && l.leader_name < r.leader_name);
  }
};

using VersionVector = std::vector<uint64_t>;

static const Name kSyncPrefix = Name("/ndn/broadcast/vsync");
static const name::Component kDataNameMarker =
    name::Component::fromEscapedString("%DA");

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_COMMON_HPP_
