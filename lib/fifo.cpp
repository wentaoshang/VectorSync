/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include "fifo.hpp"
#include "logging.hpp"
#include "vsync-helper.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.FIFONode);

namespace ndn {
namespace vsync {

void FIFONode::OnNodeData(std::shared_ptr<const Data> data) {
  const auto& n = data->getName();
  auto nid = ExtractNodeID(n);
  auto seq = ExtractSequenceNumber(n);

  auto& last_consumed_seq = last_consumed_seq_num_[nid];
  if (seq <= last_consumed_seq) {
    VSYNC_LOG_INFO("Sequence number " << seq << " from node " << nid
                                      << " already consumed in FIFO order");
    return;
  }

  auto& node_store = fifo_data_store_[nid];
  node_store[seq] = data;

  const auto& rw = recv_window_[nid].second;
  uint64_t lb = rw.LowerBound();
  assert(lb >= last_consumed_seq);
  if (lb == seq) {
    ConsumeFIFOData(node_store, last_consumed_seq, seq);
    last_consumed_seq = seq;
  }
}

void FIFONode::ConsumeFIFOData(const PerNodeDataStore& store, uint64_t begin,
                               uint64_t end) {
  VSYNC_LOG_TRACE("Consume FIFO data with sequence number in the range ("
                  << begin << ", " << end << ']');
  assert(begin < end);

  if (fifo_data_signal_.isEmpty()) {
    VSYNC_LOG_TRACE("No FIFO data signal connected");
    return;
  }

  auto iter = store.begin();
  if (begin > 0) iter = store.find(begin);
  assert(iter != store.end());
  ++iter;
  while (iter != store.end() && iter->first <= end) {
    fifo_data_signal_(iter->second);
    ++iter;
  }
}

}  // namespace vsync
}  // namespace ndn
