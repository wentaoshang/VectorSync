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

  void Insert(const uint64_t seq) { win_.insert(SeqNumInterval(seq)); }

  /**
   * @brief   Checks for missing data before sequence number @p seq.
   *
   * @param   seq  Sequence number to check
   * @return  An interval set of sequence numbers containing all the missing
   *          data.
   */
  SeqNumIntervalSet CheckForMissingData(const uint64_t seq) {
    // Ignore sequence number 0.
    if (seq == 0) return {};

    SeqNumIntervalSet r;
    r.insert(SeqNumInterval::closed(1, seq));
    r -= win_;
    return r;
  }

  bool HasAllDataBefore(uint64_t seq) const {
    return win_.iterative_size() >= 1 && win_.begin()->upper() >= seq;
  }

  /**
   * @brief  Returns the left end of the receive window, which represents
   *         the sequence number before which all data has been received.
   */
  uint64_t LowerBound() const {
    if (win_.empty())
      return 0;
    else
      return win_.begin()->upper();
  }

  /**
   * @brief  Returns the right end of the receive window, which represents
   *         the highest sequence number the node has ever announced.
   */
  uint64_t UpperBound() const {
    if (win_.empty())
      return 0;
    else
      return win_.rbegin()->upper();
  }

  friend bool operator==(const ReceiveWindow& l, const ReceiveWindow& r) {
    return l.win_ == r.win_;
  }

  friend std::ostream& operator<<(std::ostream& os, const ReceiveWindow& rw) {
    os << "ReceiveWindow=" << rw.win_;
    return os;
  }

 private:
  SeqNumIntervalSet win_;  // Intervals of received seq nums
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_RECV_WINDOW_HPP_
