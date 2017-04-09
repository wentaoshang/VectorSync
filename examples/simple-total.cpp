/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <functional>
#include <iostream>
#include <random>

#include "consensus.hpp"

namespace ndn {
namespace vsync {
namespace examples {

class SimpleTONode {
 public:
  SimpleTONode(const NodeID& nid, const Name& prefix, uint32_t seed)
      : face_(io_service_),
        scheduler_(io_service_),
        node_(face_, scheduler_, key_chain_, nid, prefix, seed),
        rengine_(seed),
        rdist_(500, 10000) {
    node_.ConnectTODataSignal(std::bind(&SimpleTONode::OnData, this, _1, _2));
  }

  void Start() {
    node_.SetConsensusGroup({"A", "B", "C"});
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                             [this] { PublishData(); });
    face_.processEvents();
  }

 private:
  void OnData(uint64_t num, std::shared_ptr<const Data> data) {
    std::cout << "Upcall OnData: Name=" << data->getName() << ", num=" << num
              << std::endl;
  }

  void PublishData() {
    node_.PublishTOData(
        "Hello from " + node_.GetNodeID(),
        [this](uint64_t num, std::shared_ptr<const Data> data) {
          scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                                   [this] { PublishData(); });
          std::cout << "Committed data: Name=" << data->getName()
                    << ", num=" << num << std::endl;
        });
  }

  boost::asio::io_service io_service_;
  Face face_;
  Scheduler scheduler_;
  KeyChain key_chain_;
  TONode node_;

  std::mt19937 rengine_;
  std::uniform_int_distribution<> rdist_;
};

int main(int argc, char* argv[]) {
  // Create a simple view with three nodes
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " [node_id] [node_prefix]"
              << std::endl;
    return -1;
  }

  NodeID nid = argv[1];
  Name prefix(argv[2]);

  std::random_device rdevice;

  SimpleTONode node(nid, prefix, static_cast<uint32_t>(rdevice()));
  node.Start();
  return 0;
}

}  // namespace examples
}  // namespace vsync
}  // namespace ndn

int main(int argc, char* argv[]) {
  return ndn::vsync::examples::main(argc, argv);
}
