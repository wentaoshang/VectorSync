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
           const Name& nid, uint32_t seed)
      : Node(face, scheduler, key_chain, nid, seed) {
    ConnectDataSignal(std::bind(&FIFONode::OnNodeData, this, _1));
  }

  void ConnectFIFODataSignal(FIFODataCb cb) {
    this->fifo_data_signal_.connect(cb);
  }

  std::shared_ptr<const Data> PublishFIFOData(const std::string& content);

 private:
  using FIFOQueue =
      std::map<uint64_t, std::pair<std::shared_ptr<const Data>, uint64_t>>;

  void OnNodeData(std::shared_ptr<const Data>);

  /**
   * @brief  Try to consume data stored in @p store with sequence number in the
   *         range (begin, end] in FIFO order.
   *
   * @param  queue  Per-node FIFO data queue
   * @param  begin  Seq num of the last consumed data from the store
   * @param  end    Seq num of the last data to be consumed
   * @return Sequence number of the last consumed data
   */
  uint64_t TryConsumeFIFOData(const FIFOQueue& queue, uint64_t begin,
                              uint64_t end);

  uint64_t last_fifo_seq_num_;
  std::unordered_map<Name, uint64_t> last_consumed_seq_num_;
  std::unordered_map<Name, FIFOQueue> fifo_data_queue_;

  FIFODataSignal fifo_data_signal_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_FIFO_HPP_
