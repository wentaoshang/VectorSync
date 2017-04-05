/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_FIFO_HPP_
#define NDN_VSYNC_FIFO_HPP_

#include <map>
#include <unordered_map>

#include "node.hpp"

namespace ndn {
namespace vsync {

class FIFONode : public Node {
 public:
  using FIFODataCb = std::function<void(std::shared_ptr<const Data>)>;

  FIFONode(Face& face, Scheduler& scheduler, KeyChain& key_chain,
           const NodeID& nid, const Name& prefix, uint32_t seed, FIFODataCb cb)
      : Node(face, scheduler, key_chain, nid, prefix, seed), app_data_cb_(cb) {
    ConnectDataSignal(std::bind(&FIFONode::OnAppData, this, _1));
  }

 private:
  using PerNodeDataStore = std::map<uint64_t, std::shared_ptr<const Data>>;

  void OnAppData(std::shared_ptr<const Data>);

  /**
   * @brief  Consume data stored in @p store with sequence number in the range
   *         (begin, end].
   *
   * @param  store  Node data store
   * @param  begin  Seq num of the last consumed data from the store
   * @param  end    Seq num of the last data to be consumed
   */
  void ConsumeData(const PerNodeDataStore& store, uint64_t begin, uint64_t end);

  std::unordered_map<NodeID, uint64_t> last_consumed_seq_num_;
  std::unordered_map<NodeID, PerNodeDataStore> app_data_store_;

  FIFODataCb app_data_cb_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_FIFO_HPP_
