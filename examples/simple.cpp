/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <functional>
#include <iostream>
#include <random>

#include "node.hpp"

namespace ndn {
namespace vsync {
namespace examples {

class SimpleNode {
 public:
  SimpleNode(const NodeID& nid, const Name& prefix, uint32_t seed)
      : face_(io_service_),
        scheduler_(io_service_),
        node_(face_, scheduler_, key_chain_, nid, prefix, seed),
        rengine_(seed),
        rdist_(500, 10000) {
    node_.ConnectDataSignal(std::bind(&SimpleNode::OnData, this, _1));
  }

  void Start() {
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                             [this] { PublishData(); });
    face_.processEvents();
  }

 private:
  void OnData(std::shared_ptr<const Data> data) {
    std::cout << "OnData: Name=" << data->getName() << std::endl;
  }

  void PublishData() {
    auto data = node_.PublishData("Hello from " + node_.GetNodeID());
    std::cout << "PublishData: Name=" << data->getName() << std::endl;
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                             [this] { PublishData(); });
  }

  boost::asio::io_service io_service_;
  Face face_;
  Scheduler scheduler_;
  KeyChain key_chain_;
  Node node_;

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

  SimpleNode node(nid, prefix, static_cast<uint32_t>(rdevice()));
  node.Start();
  return 0;
}

}  // namespace examples
}  // namespace vsync
}  // namespace ndn

int main(int argc, char* argv[]) {
  return ndn::vsync::examples::main(argc, argv);
}
