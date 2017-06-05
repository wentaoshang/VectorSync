/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_CAUSAL_HPP_
#define NDN_VSYNC_CAUSAL_HPP_

#include <map>
#include <unordered_map>

#include "node.hpp"

namespace ndn {
namespace vsync {

/**
 * @brief Causally-ordered sync node supporting causal consistency
 */
class CONode : public Node {
 public:
  using CODataSignal = util::Signal<CONode, std::shared_ptr<const Data>>;
  using CODataCb = CODataSignal::Handler;

  // Extended version vector
  using EVV = std::unordered_map<Name, uint64_t>;

  CONode(Face& face, Scheduler& scheduler, KeyChain& key_chain, const Name& nid,
         uint32_t seed)
      : Node(face, scheduler, key_chain, nid, seed) {
    ConnectDataSignal(std::bind(&CONode::OnNodeData, this, _1));
  }

  void ConnectCODataSignal(CODataCb cb) { this->co_data_signal_.connect(cb); }

  std::shared_ptr<const Data> PublishCOData(const std::string& content);

 private:
  struct EVVCompare {
    // Returns whether r dominates l (i.e., l < r)
    // Assumes all seq# in l and r are non-zero
    bool operator()(const EVV& l, const EVV& r) const {
      size_t equal_count = 0;
      for (const auto& p : l) {
        const auto& nid = p.first;
        uint64_t lseq = p.second;
        auto iter = r.find(nid);
        if (iter == r.end()) {
          // l has some node id that r doesn't have, so r cannot dominate l
          return false;
        }
        uint64_t rseq = iter->second;
        if (lseq > rseq) {
          // l's seq# is larger than r's, so r cannot dominate l
          return false;
        }
        if (lseq == rseq) ++equal_count;
      }
      // If we reach here, we know r contains all node ids in l, and the
      // corresponding seq# in r are either greater than or equal to those in l
      if (r.size() > l.size()) {
        // r has some node id that l doesn't have, so r dominates l
        return true;
      } else {
        // l and r must have equal size
        if (equal_count == l.size()) {
          // all components are equal, so l == r
          return false;
        } else {
          return true;
        }
      }
    }
  };

  using EVVQueue = std::map<EVV, std::shared_ptr<const Data>, EVVCompare>;

  void OnNodeData(std::shared_ptr<const Data>);

  void ConsumeCOData(std::shared_ptr<const Data>, const EVV&);

  EVV causal_cut_;
  EVVQueue pending_co_data_;

  CODataSignal co_data_signal_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_CAUSAL_HPP_
