/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include "fifo.hpp"
#include "logging.hpp"
#include "vsync-helper.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.FIFONode);

namespace ndn {
namespace vsync {

std::shared_ptr<const Data> FIFONode::PublishFIFOData(
    const std::string& content) {
  proto::FIFOData fod_proto;
  fod_proto.set_content(content);
  fod_proto.set_prev(last_fifo_seq_num_);

  auto d = PublishData(fod_proto.SerializeAsString());
  VSYNC_LOG_TRACE("Publish: FIFOData.Name=" << d->getName()
                                            << ", prev=" << last_fifo_seq_num_);
  last_fifo_seq_num_ = ExtractSequenceNumber(d->getName());
  return d;
}

void FIFONode::OnNodeData(std::shared_ptr<const Data> data) {
  const auto& n = data->getName();
  VSYNC_LOG_TRACE("Recv: FIFOData: d.name=" << n);
  auto nid = ExtractNodeID(n);
  auto seq = ExtractSequenceNumber(n);

  auto& last_consumed_seq = last_consumed_seq_num_[nid];
  if (seq <= last_consumed_seq) {
    VSYNC_LOG_INFO("Sequence number " << seq << " from node " << nid
                                      << " already consumed in FIFO order");
    return;
  }

  proto::FIFOData fod_proto;
  if (!fod_proto.ParseFromArray(data->getContent().value(),
                                data->getContent().value_size())) {
    VSYNC_LOG_WARN("Cannot parse FIFOData: d.name=" << n);
    return;
  }
  uint64_t prev_seq = fod_proto.prev();

  auto& queue = fifo_data_queue_[nid];
  queue[seq] = {data, prev_seq};
  last_consumed_seq = TryConsumeFIFOData(queue, last_consumed_seq, seq);
}

uint64_t FIFONode::TryConsumeFIFOData(const FIFOQueue& queue, uint64_t begin,
                                      uint64_t end) {
  VSYNC_LOG_TRACE("Try to consume FIFOData with sequence number in the range ("
                  << begin << ", " << end << ']');
  assert(begin < end);

  auto iter = queue.upper_bound(begin);
  uint64_t last_consumed = begin;
  while (iter != queue.end() && iter->first <= end) {
    auto& data = iter->second.first;
    uint64_t prev_seq = iter->second.second;
    if (prev_seq != last_consumed) break;

    VSYNC_LOG_TRACE("Consume: FIFOData.Name=" << data->getName()
                                              << ", prev_seq=" << prev_seq);
    fifo_data_signal_(data);

    last_consumed = iter->first;
    ++iter;
  }
  return last_consumed;
}

}  // namespace vsync
}  // namespace ndn
