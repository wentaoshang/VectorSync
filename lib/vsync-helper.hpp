/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_INTEREST_HELPER_HPP_
#define NDN_VSYNC_INTEREST_HELPER_HPP_

#include <iostream>
#include <iterator>
#include <limits>
#include <sstream>

#include <ndn-cxx/name.hpp>

#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

// Hepbers for view id

inline std::ostream& operator<<(std::ostream& os, const ViewID& vi) {
  return os << '(' << vi.first << ',' << vi.second << ')';
}

inline std::string ToString(const ViewID& vi) {
  std::ostringstream os;
  os << '(' << vi.first << ',' << vi.second << ')';
  return os.str();
}

// Helpers for version vector processing

inline VersionVector Merge(const VersionVector& v1, const VersionVector& v2) {
  if (v1.size() != v2.size()) return {};

  VersionVector res(v1.size());
  std::transform(v1.begin(), v1.end(), v2.begin(), res.begin(),
                 [](uint64_t l, uint64_t r) { return std::max(l, r); });
  return res;
}

inline std::ostream& operator<<(std::ostream& os, const VersionVector& v) {
  os << '[';
  for (size_t i = 0; i != v.size(); ++i) {
    os << v[i];
    if (i != v.size() - 1) os << ',';
  }
  os << ']';
  return os;
}

inline std::string ToString(const VersionVector& v) {
  std::string s(1, '[');
  s.append(std::accumulate(std::next(v.begin()), v.end(), std::to_string(v[0]),
                           [](std::string a, uint64_t b) {
                             return a + ',' + std::to_string(b);
                           }))
      .append(1, ']');
  return s;
}

inline void EncodeVV(const VersionVector& v, proto::VV* vv_proto) {
  for (uint64_t n : v) {
    vv_proto->add_entry(n);
  }
}

inline void EncodeVV(const VersionVector& v, std::string& out) {
  proto::VV vv_proto;
  EncodeVV(v, &vv_proto);
  vv_proto.AppendToString(&out);
}

inline VersionVector DecodeVV(const proto::VV& vv_proto) {
  VersionVector vv(vv_proto.entry_size(), 0);
  for (int i = 0; i < vv_proto.entry_size(); ++i) {
    vv[i] = vv_proto.entry(i);
  }
  return vv;
}

inline VersionVector DecodeVV(const void* buf, size_t buf_size) {
  proto::VV vv_proto;
  if (!vv_proto.ParseFromArray(buf, buf_size)) return {};
  return DecodeVV(vv_proto);
}

struct VVCompare {
  bool operator()(const VersionVector& l, const VersionVector& r) const {
    if (l.size() != r.size()) return false;
    size_t equal_count = 0;
    for (size_t i = 0; i < l.size(); ++i) {
      if (l[i] > r[i])
        return false;
      else if (l[i] == r[i])
        ++equal_count;
    }
    if (equal_count == l.size())
      return false;
    else
      return true;
  }
};

// Helpers for interest processing

inline Name MakeSyncInterestName(const ViewID& vid, const std::string& digest) {
  // name = /[vsync_prefix]/digest/[view_num]/[leader_id]/[vector_clock_digest]
  Name n(kSyncPrefix);
  n.append("digest").appendNumber(vid.first).append(vid.second).append(digest);
  return n;
}

inline Name MakeVectorInterestName(const Name& sync_interest_name) {
  // name = /[vsync_prefix]/vector/[view_num]/[leader_id]/[vector_clock_digest]
  Name n(kSyncPrefix);
  n.append("vector")
      .append(sync_interest_name.get(-3))
      .append(sync_interest_name.get(-2))
      .append(sync_interest_name.get(-1));
  return n;
}

inline Name MakeViewInfoName(const ViewID& vid) {
  // name = /[vsync_prefix]/vinfo/[view_num]/[leader_id]/%00
  Name n(kSyncPrefix);
  n.append("vinfo").appendNumber(vid.first).append(vid.second).appendNumber(0);
  return n;
}

inline ViewID ExtractViewID(const Name& n) {
  uint64_t view_num = n.get(-3).toNumber();
  std::string leader_id = n.get(-2).toUri();
  return {view_num, leader_id};
}

inline std::string ExtractVectorDigest(const Name& n) {
  return n.get(-1).toUri();
}

// Helpers for data processing

inline Name MakeDataName(const Name& prefix, const NodeID& nid, uint64_t seq) {
  // name = /[node_prefix]/[node_id]/[seq_num]
  Name n(prefix);
  n.append(nid).appendNumber(seq);
  return n;
}

inline Name ExtractNodePrefix(const Name& n) { return n.getPrefix(-2); }

inline NodeID ExtractNodeID(const Name& n) { return n.get(-2).toUri(); }

inline uint64_t ExtractSequenceNumber(const Name& n) {
  return n.get(-1).toNumber();
}

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_INTEREST_HELPER_HPP_
