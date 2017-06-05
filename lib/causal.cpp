/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include "causal.hpp"
#include "logging.hpp"
#include "vsync-helper.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.CONode);

namespace ndn {
namespace vsync {

std::shared_ptr<const Data> CONode::PublishCOData(const std::string& content) {
  proto::COData cod_proto;
  cod_proto.set_content(content);

  VSYNC_LOG_TRACE("Publish: COData.EVV=[");
  auto* evv_proto = cod_proto.mutable_evv();
  for (const auto& p : causal_cut_) {
    if (p.second == 0) continue;  // do no encode zero seq#
    auto* entry_proto = evv_proto->add_entry();
    entry_proto->set_nid(p.first.toUri());
    entry_proto->set_seq(p.second);
    VSYNC_LOG_TRACE("    " << p.first << ':' << p.second);
  }
  VSYNC_LOG_TRACE(']');

  auto d = PublishData(cod_proto.SerializeAsString());
  VSYNC_LOG_TRACE("Publish: COData.Name=" << d->getName());

  causal_cut_[nid_] = vv_[idx_];
  VSYNC_LOG_TRACE("Update: causal_cut=[");
  for (const auto& p : causal_cut_) {
    VSYNC_LOG_TRACE("    " << p.first << ':' << p.second);
  }
  VSYNC_LOG_TRACE(']');

  return d;
}

static std::pair<CONode::EVV, bool> ExtractEVV(const Block& content) {
  proto::COData cod_proto;
  if (!cod_proto.ParseFromArray(content.value(), content.value_size()))
    return {};
  CONode::EVV ret;
  for (int i = 0; i < cod_proto.evv().entry_size(); ++i) {
    const auto& entry = cod_proto.evv().entry(i);
    ret[entry.nid()] = entry.seq();
  }
  return {ret, true};
}

void CONode::OnNodeData(std::shared_ptr<const Data> data) {
  VSYNC_LOG_TRACE("Recv: COData.Name=" << data->getName());
  auto p = ExtractEVV(data->getContent());
  if (!p.second) {
    VSYNC_LOG_WARN("Cannot parse COData: d.name=" << data->getName());
    return;
  }
  const auto& evv = p.first;
  EVVCompare comp;
  if (comp(evv, causal_cut_) || evv == causal_cut_) {
    // evv <= causal_cut_, so this data can be consumed
    ConsumeCOData(data, evv);

    // Check pending data to see if any of them can be consumed now (based on
    // the new causal cut). Repeat this process until no more pending data can
    // be consumed.
    auto iter = pending_co_data_.upper_bound(causal_cut_);
    while (iter != pending_co_data_.begin()) {
      for (auto it = pending_co_data_.begin(); it != iter; ++it) {
        ConsumeCOData(it->second, it->first);
      }
      pending_co_data_.erase(pending_co_data_.begin(), iter);
      iter = pending_co_data_.upper_bound(causal_cut_);
    }
  } else {
    // Some data that is causally ordered before this data has not been received
    // yet. Add this data to the pending data queue to be processed later.
    pending_co_data_.insert({evv, data});
    VSYNC_LOG_TRACE("Enqueue: pending COData.Name=" << data->getName());
  }
}

void CONode::ConsumeCOData(std::shared_ptr<const Data> data, const EVV& evv) {
  const auto& n = data->getName();
  VSYNC_LOG_TRACE("Consume: COData.Name=" << n);
  auto nid = ExtractNodeID(n);
  uint64_t seq = ExtractSequenceNumber(n);

  VSYNC_LOG_TRACE("Before: causal_cut=[");
  for (const auto& p : causal_cut_) {
    VSYNC_LOG_TRACE("    " << p.first << ':' << p.second);
  }
  VSYNC_LOG_TRACE(']');

  VSYNC_LOG_TRACE("Consume: COData.EVV=[");
  for (const auto& p : evv) {
    VSYNC_LOG_TRACE("    " << p.first << ':' << p.second);
    // For any data with seq# S from node N, the seq# of N in the EVV of that
    // data must be (S - 1).
    if (p.first == nid) assert(p.second == seq - 1);
  }
  VSYNC_LOG_TRACE(']');

  auto& s = causal_cut_[nid];
  // If we can consume data with seq# S from a node N, we must have already
  // consumed data with seq# up to (S - 1) from N.
  assert(s == seq - 1);
  s = seq;
  co_data_signal_(data);
  VSYNC_LOG_TRACE("After: causal_cut=[");
  for (const auto& p : causal_cut_) {
    VSYNC_LOG_TRACE("    " << p.first << ':' << p.second);
  }
  VSYNC_LOG_TRACE(']');
}

}  // namespace vsync
}  // namespace ndn
