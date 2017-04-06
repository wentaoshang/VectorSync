/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_FIFO_HPP_
#define NDN_VSYNC_FIFO_HPP_

#include <map>
#include <unordered_map>

#include "node.hpp"

namespace ndn {
namespace vsync {

/**
 * @brief FIFO-ordered sync node supporting FIFO consistency
 */
class FIFONode : public Node {
 public:
  using FIFODataSignal = util::Signal<FIFONode, std::shared_ptr<const Data>>;
  using FIFODataCb = FIFODataSignal::Handler;

  FIFONode(Face& face, Scheduler& scheduler, KeyChain& key_chain,
           const NodeID& nid, const Name& prefix, uint32_t seed)
      : Node(face, scheduler, key_chain, nid, prefix, seed) {
    ConnectDataSignal(std::bind(&FIFONode::OnNodeData, this, _1));
  }

  void ConnectFIFODataSignal(FIFODataCb cb) {
    this->fifo_data_signal_.connect(cb);
  }

 private:
  using PerNodeDataStore = std::map<uint64_t, std::shared_ptr<const Data>>;

  void OnNodeData(std::shared_ptr<const Data>);

  /**
   * @brief  Consume data stored in @p store with sequence number in the range
   *         (begin, end] in FIFO order.
   *
   * @param  store  Node data store
   * @param  begin  Seq num of the last consumed data from the store
   * @param  end    Seq num of the last data to be consumed
   */
  void ConsumeFIFOData(const PerNodeDataStore& store, uint64_t begin,
                       uint64_t end);

  std::unordered_map<NodeID, uint64_t> last_consumed_seq_num_;
  std::unordered_map<NodeID, PerNodeDataStore> fifo_data_store_;

  FIFODataSignal fifo_data_signal_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_FIFO_HPP_
