/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_NODE_HPP_
#define NDN_VSYNC_NODE_HPP_

#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <tuple>
#include <unordered_map>

#include "ndn-common.hpp"
#include "recv-window.hpp"
#include "view-info.hpp"
#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

void SetInterestLifetime(const time::milliseconds sync_interest_lifetime,
                         const time::milliseconds data_interest_lifetime);

void SetHeartbeatInterval(const time::milliseconds heartbeat_interval);

class Node {
 public:
  using DataSignal = util::Signal<Node, std::shared_ptr<const Data>>;
  using DataCb = DataSignal::Handler;
  using VectorChangeSignal =
      util::Signal<Node, std::size_t, const VersionVector&>;
  using VectorChangeCb = VectorChangeSignal::Handler;
  using ViewChangeSignal =
      util::Signal<Node, const ViewID&, const ViewInfo&, bool>;
  using ViewChangeCb = ViewChangeSignal::Handler;

  enum DataType : uint32_t {
    kUserData = 0,
    kLastDataInfo = 9666,
    kHeartbeat = 9667,
    kSyncReply = 9668,
    kViewInfo = 9669,
    kVectorClock = 9670,
    kNodeSnapshot = 9671,
    kGroupSnapshot = 9672,
  };

  class Error : public std::exception {
   public:
    Error(const std::string& what) : what_(what) {}

    virtual const char* what() const noexcept override { return what_.c_str(); }

   private:
    std::string what_;
  };

  /**
   * @brief Creates a VSync node in a default view with itself as the only
   *        member. The node itself is set as the leader of the view, which has
   *        view ID (1, @p nid). View number always starts from 1. 0 is reserved
   *        for representing invalid view number.
   *
   * @param face       Reference to the Face object on which the node runs
   * @param scheduler  Reference to the scheduler associated with @p face
   * @param key_chain  Reference to the KeyChain object used by the application
   * @param nid        Unique node name (also serves as data prefix)
   * @param seed       Value to seed the PRNG engine
   */
  Node(Face& face, Scheduler& scheduler, KeyChain& key_chain, const Name& nid,
       uint32_t seed);

  void SetViewInfo(const ViewID& vid, const ViewInfo& vinfo) {
    vid_ = vid;
    vinfo_ = vinfo;
  }

  void Start();

  const Name& GetNodeID() const { return nid_; }

  std::size_t GetNodeIndex() const { return idx_; }

  bool LoadView(const ViewID& vid, const ViewInfo& vinfo);

  std::shared_ptr<const Data> PublishData(const std::string& content,
                                          uint32_t type = kUserData);

  void ConnectDataSignal(DataCb cb) { this->data_signal_.connect(cb); }

  void ConnectVectorChangeSignal(VectorChangeCb cb) {
    this->vector_change_signal_.connect(cb);
  }

  void ConnectViewChangeSignal(ViewChangeCb cb) {
    this->view_change_signal_.connect(cb);
  }

 protected:
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  void ResetState();

  inline void SendSyncInterest();
  void OnSyncInterestTimeout(const Interest& interest, int retry_count);
  inline void SendDataInterest(const Name& data_name);
  inline void SendDataInterest(const Name& nid, uint64_t seq);
  void OnDataInterestTimeout(const Interest& interest, int retry_count);
  inline void SendSyncReply(const Name& n);
  void ProcessSyncReply(const Interest& inst, const Data& reply);

  inline void PublishHeartbeat();
  inline void ProcessHeartbeat(const ViewID& vid, const Name& nid);

  void OnSyncInterest(const Interest& interest);
  void OnDataInterest(const Interest& interest);
  void OnRemoteData(const Data& data);

  void ProcessState(const Name& nid, uint64_t seq, const ViewID& vid,
                    const VersionVector& vv);

  /**
   * @brief Adds the sequence number @p seq of the received data into the
   *        receive window of node @p nid and slides the window forward until
   *        there is a hole (i.e., two non-consecutive seq#) in the window.
   *        Then send Data Interests to fetch missising data with sequence
   *        number smaller than @p seq.
   *
   * @param nid    Remote node name
   * @param seq    Sequence number of @p data
   */
  void UpdateReceiveWindow(const Name& nid, uint64_t seq);

  /**
   * @brief Processes the received @p vid and decide if there is a need to
   *        perform view change.
   *
   * @param vid    Received view id that is different from our own
   */
  void DoViewChange(const ViewID& vid);

  void OnViewInfoInterestTimeout(const Interest& interest, int retry_count);
  void ProcessViewInfo(const Interest& vinterest, const Data& vinfo);
  inline void PublishViewInfo();
  void DoHealthcheck();

  /*
  void PublishNodeSnapshot();
  void ProcessNodeSnapshot(const std::string& content, const NodeID& nid);
  void PublishGroupSnapshot();
  */
  Face& face_;
  Scheduler& scheduler_;
  KeyChain& key_chain_;

  const Name nid_;
  std::size_t idx_ = 0;
  bool is_leader_ = true;
  ViewID vid_;
  ViewInfo vinfo_;
  VersionVector vv_;

  // Hash table mapping node ID to its receive window
  std::unordered_map<Name, ReceiveWindow> recv_window_;

  // In-memory store for all data
  std::unordered_map<Name, std::shared_ptr<const Data>> data_store_;

  // Snapshot of the entire dataset
  // std::unordered_map<Name, uint64_t> snapshot_;
  // std::vector<bool> node_snapshot_bitmap_;

  std::mt19937 rengine_;
  std::uniform_int_distribution<> heartbeat_random_delay_;

  util::scheduler::ScopedEventId heartbeat_event_;
  util::scheduler::ScopedEventId healthcheck_event_;
  std::vector<time::steady_clock::TimePoint> last_heartbeat_;

  DataSignal data_signal_;
  ViewChangeSignal view_change_signal_;
  VectorChangeSignal vector_change_signal_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_NODE_HPP_
