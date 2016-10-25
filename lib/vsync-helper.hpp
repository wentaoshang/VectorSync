/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_INTEREST_HELPER_HPP_
#define NDN_VSYNC_INTEREST_HELPER_HPP_

#include <iterator>
#include <limits>
#include <sstream>

#include <ndn-cxx/name.hpp>

#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

inline std::string ToString(const ESN& s) {
  std::ostringstream os;
  os << "{(" << s.vi.first << ',' << s.vi.second << ")," << s.rn
     << ',' << s.seq << '}';
  return os.str();
}

// Helpers for version vector processing

inline VersionVector Merge(const VersionVector& v1, const VersionVector& v2) {
  if (v1.size() != v2.size())
    return {};

  VersionVector res(v1.size());
  std::transform(v1.begin(), v1.end(), v2.begin(), res.begin(),
                 [](uint64_t l, uint64_t r) { return std::max(l, r); });
  return res;
}

inline std::string ToString(const VersionVector& v) {
  std::string s(1, '[');
  s.append(std::accumulate(std::next(v.begin()), v.end(),
                           std::to_string(v[0]),
                           [](std::string a, uint8_t b) {
                             return a + ',' + std::to_string(b);
                           })).append(1, ']');
  return s;
}

// Helpers for sync Interest processing

inline Name MakeVsyncInterestName(const ViewID& vid, uint64_t rn,
                                  const VersionVector& vv) {
  // name = /vsync_prefix/view_num/leader_id/round_num/version_vector
  Name n(kVsyncPrefix);
  n.appendNumber(vid.first).append(vid.second)
    .appendNumber(rn).append(name::Component(vv.begin(), vv.end()));
  return n;
}

inline Name MakeViewInfoName(const ViewID& vid) {
  // name = /[vsync_prefix]/[view_num]/[leader_id]/vinfo
  Name n(kVsyncPrefix);
  n.appendNumber(vid.first).append(vid.second).append("vinfo");
  return n;
}

inline ViewID ExtractViewID(const Name& n) {
  uint64_t view_num = n.get(-4).toNumber();
  std::string leader_id = n.get(-3).toUri();
  return {view_num, leader_id};
}

inline ViewID ExtractViewIDFromViewInfoName(const Name& n) {
  uint64_t view_num = n.get(-3).toNumber();
  std::string leader_id = n.get(-2).toUri();
  return {view_num, leader_id};
}

inline uint64_t ExtractRoundNumber(const Name& n) {
  return n.get(-2).toNumber();
}

inline VersionVector ExtractVersionVector(const Name& n) {
  const auto& comp = n.get(-1);
  return VersionVector(comp.value_begin(), comp.value_end());
}

// Helpers for data processing

inline Name MakeDataName(const Name& prefix, const NodeID& nid, const ViewID& vid,
                         uint64_t rn, uint8_t seq) {
  // name = /node_prefix/node_id/view_num/leader_id/round_num/seq_num
  Name n(prefix);
  n.append(nid).appendNumber(vid.first).append(vid.second)
    .appendNumber(rn).appendNumber(seq);
  return n;
}

inline NodeID ExtractNodeID(const Name& n) {
  return n.get(-5).toUri();
}

inline uint64_t ExtractSequenceNumber(const Name& n) {
  return n.get(-1).toNumber();
}

inline void EncodeESN(const ESN& ldi,
                      std::string& out) {
  proto::ESN esn_proto;
  esn_proto.set_view_num(ldi.vi.first);
  esn_proto.set_leader_id(ldi.vi.second);
  esn_proto.set_round_num(ldi.rn);
  esn_proto.set_seq_num(ldi.seq);
  esn_proto.AppendToString(&out);
}

inline std::pair<ESN, bool> DecodeESN(const void* buf,
                                      size_t buf_size) {
  proto::ESN esn_proto;
  if (!esn_proto.ParseFromArray(buf, buf_size))
    return {};

  ESN esn;
  esn.vi.first = esn_proto.view_num();
  esn.vi.second = esn_proto.leader_id();
  esn.rn = esn_proto.round_num();
  esn.seq = esn_proto.seq_num();
  return {esn, true};
}

} // namespace vsync
} // namespace ndn

#endif // NDN_VSYNC_INTEREST_HELPER_HPP_
