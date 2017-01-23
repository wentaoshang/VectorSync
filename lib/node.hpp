/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_NODE_HPP_
#define NDN_VSYNC_NODE_HPP_

#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <random>
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
  using DataCb = std::function<void(const std::string&, const ViewID&,
                                    const VersionVector&)>;

  enum DataType : uint32_t {
    kUserData = 0,
    kLastDataInfo = 9666,
    kHeartbeat = 9667,
    kSyncReply = 9668,
    kViewInfo = 9669,
    kVectorClock = 9670,
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

  const NodeID& GetNodeID() const { return id_; }

  bool LoadView(const ViewID& vid, const ViewInfo& vinfo);

  VersionVector PublishData(const std::string& content,
                            uint32_t type = kUserData);

  void PrintCausalityGraph() const;

 private:
  using VVQueue =
      std::map<VersionVector, std::shared_ptr<const Data>, VVCompare>;

  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  void ResetState();

  inline void SendSyncInterest();
  inline void SendDataInterest(const Name& prefix, const NodeID& nid,
                               uint64_t seq);
  inline void SendSyncReply(const Name& n);
  inline void PublishHeartbeat();
  inline void ProcessHeartbeat(const ViewID& vid, const NodeID& nid);

  void OnSyncInterest(const Interest& interest);
  void OnDataInterest(const Interest& interest);
  void OnRemoteData(const Data& data);

  void SendVectorInterest(const Name& sync_interest_name);
  void PublishVector(const Name& sync_interest_name, const std::string& digest);
  void ProcessVector(const Data& data);

  /**
   * @brief Adds the extended sequence number of @p data into the receive
   *        window of node @p nid and slides the window forward until
   *        there is a hole (i.e., two non-consecutive esns) in the window.
   *
   * @param data   Received data from remote node
   * @param nid    Node ID of the sender of @p data
   * @param seq    Sequence number of @p data
   */
  void UpdateReceiveWindow(const Data& data, const NodeID& nid, uint64_t seq);
  VersionVector GenerateDataVV() const;

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
  VersionVector vector_clock_;

  // Hash table mapping node ID to its receive window
  std::unordered_map<NodeID, ReceiveWindow> recv_window_;

  // Ordered map mapping view id to a hash table that maps node id to its
  // version vector queue
  std::map<ViewID, std::unordered_map<NodeID, VVQueue>> causality_graph_;

  std::unordered_map<Name, std::shared_ptr<const Data>> data_store_;
  DataCb data_cb_;

  std::random_device rdevice_;
  std::mt19937 rengine_;
  std::uniform_int_distribution<> rdist_;

  util::scheduler::ScopedEventId heartbeat_event_;
  util::scheduler::ScopedEventId healthcheck_event_;
  util::scheduler::ScopedEventId leader_election_event_;
  std::vector<time::steady_clock::TimePoint> last_heartbeat_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_NODE_HPP_
