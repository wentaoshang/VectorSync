/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <functional>
#include <random>

#include "consensus.hpp"
#include "logging.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.examples.SimpleTONode);

namespace ndn {
namespace vsync {
namespace examples {

class SimpleTONode {
 public:
  SimpleTONode(const Name& nid, uint32_t seed)
      : face_(io_service_),
        scheduler_(io_service_),
        node_(face_, scheduler_, key_chain_, nid, seed),
        rengine_(seed),
        rdist_(500, 10000) {
    node_.ConnectTODataSignal(std::bind(&SimpleTONode::OnData, this, _1, _2));
  }

  void Start() {
    node_.SetConsensusGroup({"A", "B", "C"});
    node_.Start();
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                             [this] { PublishData(); });
    face_.processEvents();
  }

 private:
  void OnData(uint64_t num, std::shared_ptr<const Data> data) {
    VSYNC_LOG_TRACE("OnData: Name=" << data->getName() << ", num=" << num);
  }

  void PublishData() {
    std::string msg =
        node_.GetNodeID().toUri() + ":" + std::to_string(++counter_);
    node_.PublishTOData(
        msg, [this](uint64_t num, std::shared_ptr<const Data> data) {
          scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                                   [this] { PublishData(); });
          VSYNC_LOG_TRACE("Committed data: Name=" << data->getName()
                                                  << ", num=" << num);
        });
  }

  boost::asio::io_service io_service_;
  Face face_;
  Scheduler scheduler_;
  KeyChain key_chain_;
  TONode node_;

  uint64_t counter_ = 0;

  std::mt19937 rengine_;
  std::uniform_int_distribution<> rdist_;
};

int main(int argc, char* argv[]) {
  // Create a simple view with three nodes
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " [node_id]" << std::endl;
    return -1;
  }

  Name nid{argv[1]};

  std::random_device rdevice;

  SimpleTONode node(nid, static_cast<uint32_t>(rdevice()));
  node.Start();
  return 0;
}

}  // namespace examples
}  // namespace vsync
}  // namespace ndn

int main(int argc, char* argv[]) {
  return ndn::vsync::examples::main(argc, argv);
}
