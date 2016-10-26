/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_NODE_HPP_
#define NDN_VSYNC_NODE_HPP_

#include <exception>
#include <functional>
#include <memory>
#include <set>
#include <unordered_map>

#include "ndn-common.hpp"
#include "recv-window.hpp"
#include "view-info.hpp"
#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

class Node {
 public:
  using DataCb = std::function<void(const uint8_t*, size_t)>;

  enum DataType : uint32_t {
    kUserData = 0,
    kLastDataInfo = 9666,
    kHeartbeat = 9667,
    kVsyncReply = 9668,
    kViewInfo = 9669,
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
   * @param nid        Unique node ID
   * @param prefix     Data prefix of the node
   * @param on_data    Callback for notifying new data to the application
   */
  Node(Face& face, Scheduler& scheduler, KeyChain& key_chain, const NodeID& nid,
       const Name& prefix, DataCb on_data);

  bool LoadView(const ViewID& vid, const ViewInfo& vinfo);

  void PublishData(const std::vector<uint8_t>& content,
                   uint32_t type = kUserData);

 private:
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  void ResetState() {
    idx_ = view_info_.GetIndexByID(id_).first;
    is_leader_ = view_id_.second == id_;
    version_vector_.clear();
    version_vector_.resize(view_info_.Size());
    member_state_.clear();
    auto now = time::steady_clock::now();
    member_state_.resize(view_info_.Size(), {{}, now});
  }

  inline void SendSyncInterest();
  inline void SendDataInterest(const Name& prefix, const NodeID& nid,
                               const ViewID& vid, uint64_t seq);
  inline void SendSyncReply(const Name& n);
  inline void PublishHeartbeat();
  inline void ProcessHeartbeat(NodeIndex index);

  void OnSyncInterest(const Interest& interest);
  void OnDataInterest(const Interest& interest);
  void OnRemoteData(const Data& data);

  /**
   * @brief Adds the extended sequence number of @p data into the receive
   *        window of node @p index and slides the window forward until
   *        there is a hole (i.e., two non-consecutive esns) in the window.
   *
   * @param data   Received data from remote node
   * @param index  Node index of the sender of @p data
   */
  void UpdateReceiveWindow(const Data& data, NodeIndex index);

  void DoViewChange(const ViewID& vid);
  void ProcessViewInfo(const Interest& vinterest, const Data& vinfo);
  inline void PublishViewInfo();
  void DoHealthcheck();

  inline void ProcessLeaderElectionTimeout();

  Face& face_;
  Scheduler& scheduler_;
  KeyChain& key_chain_;
  const NodeID id_;
  Name prefix_;
  NodeIndex idx_ = 0;
  bool is_leader_ = true;
  ViewID view_id_;
  ViewInfo view_info_;
  VersionVector version_vector_;

  ESN last_data_info_ = {};

  struct MemberState {
    ReceiveWindow recv_window;
    time::steady_clock::TimePoint last_heartbeat;
  };
  std::vector<MemberState> member_state_;

  std::unordered_map<Name, std::shared_ptr<const Data>> data_store_;
  DataCb data_cb_;

  std::random_device rdevice_;
  std::mt19937 rengine_;
  std::uniform_int_distribution<> rdist_;

  util::scheduler::ScopedEventId heartbeat_event_;
  util::scheduler::ScopedEventId healthcheck_event_;
  util::scheduler::ScopedEventId leader_election_event_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_NODE_HPP_
