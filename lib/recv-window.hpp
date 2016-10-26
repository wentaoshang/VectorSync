/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_RECV_WINDOW_HPP_
#define NDN_VSYNC_RECV_WINDOW_HPP_

#include <iostream>
#include <iterator>
#include <map>
#include <string>

#include <boost/icl/interval_set.hpp>

#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

class ReceiveWindow {
 public:
  using SeqNumInterval = boost::icl::discrete_interval<uint64_t>;
  using SeqNumIntervalSet = boost::icl::interval_set<uint64_t>;

  bool Insert(const ESN& esn) {
    if (esn.vi.first == 0 || esn.vi.second.empty() || esn.seq == 0)
      return false;

    auto& entry = state_[esn.vi.first];
    if (!entry.leader_id.empty()) {
      // If the entry for the same view number exits,
      // the leader_id must be the same.
      if (entry.leader_id != esn.vi.second) return false;
    } else {
      // This is newly created entry. Set leader id now.
      entry.leader_id = esn.vi.second;
    }

    // Insert seq number into the window
    entry.win.insert(SeqNumInterval(esn.seq));
    return true;
  }

  /**
   * @brief Given the last data info in @p ldi, checks for missing data in
   *        the same view as @p ldi.
   *
   * @param ldi  Last data info represented as ESN
   * @param vid  View ID of the view in which @p ldi was received
   */
  SeqNumIntervalSet CheckForMissingData(const ESN& ldi, const ViewID& vid) {
    // Ignore last data info with view/seq number equal to 0.
    if (ldi.vi.first == 0 || ldi.seq == 0) return {};

    // vid must be greater than ldi.vid.
    if (vid.first <= ldi.vi.first) return {};

    SeqNumIntervalSet r;
    r.insert(SeqNumInterval::closed(1, ldi.seq));
    auto& entry = state_[ldi.vi.first];
    if (!entry.leader_id.empty()) {
      if (entry.leader_id != ldi.vi.second) return {};
    } else {
      entry.leader_id = ldi.vi.second;
    }
    if (entry.last_seq_num != 0) {
      // Last data info is already set. Check if it is consistent with
      // the input parameters.
      if (entry.last_seq_num != ldi.seq) return {};
      if (entry.next_vi != vid) return {};
    } else {
      entry.last_seq_num = ldi.seq;
      entry.next_vi = vid;
    }
    r -= entry.win;
    return r;
  }

  friend bool operator==(const ReceiveWindow& l, const ReceiveWindow& r) {
    return l.state_ == r.state_;
  }

  friend std::ostream& operator<<(std::ostream& os, const ReceiveWindow& rw) {
    os << "ReceiveWindow{";
    for (const auto& p : rw.state_) {
      os << "[vi=(" << p.first << "," << p.second.leader_id
         << "),win=" << p.second.win << "]";
    }
    os << "}";
    return os;
  }

 private:
  struct Entry {
    std::string leader_id;  // Leader id of this view
    SeqNumIntervalSet win;  // Intervals of received seq nums
    ViewID next_vi;         // View number of the next round after this
    uint64_t last_seq_num;  // Last sequence number in this round

    friend bool operator==(const Entry& l, const Entry& r) {
      return l.leader_id == r.leader_id && l.win == r.win &&
             l.next_vi == r.next_vi && l.last_seq_num == r.last_seq_num;
    }
  };

  std::map<uint64_t, Entry> state_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_RECV_WINDOW_HPP_
