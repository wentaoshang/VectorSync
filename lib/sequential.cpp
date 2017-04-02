/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include "sequential.hpp"
#include "logging.hpp"
#include "vsync-helper.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.SONode);

namespace ndn {
namespace vsync {

void SONode::OnAppData(std::shared_ptr<const Data> data) {
  const auto& n = data->getName();
  auto nid = ExtractNodeID(n);
  auto seq = ExtractSequenceNumber(n);

  auto& last_consumed_seq = last_consumed_seq_num_[nid];
  if (seq <= last_consumed_seq) {
    VSYNC_LOG_INFO("Sequence number " << seq << " already consumed by app");
    return;
  }

  auto& node_store = app_data_store_[nid];
  node_store[seq] = data;

  const auto& rw = recv_window_[nid];
  uint64_t lb = rw.LowerBound();
  assert(lb >= last_consumed_seq);
  if (lb == seq) {
    ConsumeData(node_store, last_consumed_seq, seq);
    last_consumed_seq = seq;
  }
}

void SONode::ConsumeData(const PerNodeDataStore& store, uint64_t begin,
                         uint64_t end) {
  VSYNC_LOG_TRACE("Consume SC data with sequence number in the range ("
                  << begin << ", " << end << ']');
  assert(begin < end);
  auto iter = store.begin();
  if (begin > 0) iter = store.find(begin);
  assert(iter != store.end());
  ++iter;
  if (app_data_cb_) {
    while (iter != store.end() && iter->first <= end) {
      app_data_cb_(iter->second);
      ++iter;
    }
  }
}

}  // namespace vsync
}  // namespace ndn
