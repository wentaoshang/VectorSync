/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <functional>
#include <iostream>
#include <random>

#include "vsync.hpp"

namespace ndn {
namespace vsync {
namespace examples {

class SimpleNode {
 public:
  SimpleNode(const NodeID& nid, const Name& prefix)
      : face_(io_service_),
        scheduler_(io_service_),
        node_(face_, scheduler_, key_chain_, nid, prefix,
              std::bind(&SimpleNode::OnData, this, _1, _2)),
        rengine_(rdevice_()),
        rdist_(500, 10000) {}

  void Start() {
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                             [this] { PublishData(); });
    face_.processEvents();
  }

 private:
  void OnData(const uint8_t* buf, size_t buf_size) {
    std::cout << "Upcall OnData: content_size=" << buf_size << std::endl;
  }

  void PublishData() {
    node_.PublishData({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                             [this] { PublishData(); });
  }

  boost::asio::io_service io_service_;
  Face face_;
  Scheduler scheduler_;
  KeyChain key_chain_;
  Node node_;

  std::random_device rdevice_;
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

  SimpleNode node(nid, prefix);
  node.Start();
  return 0;
}

}  // namespace examples
}  // namespace vsync
}  // namespace ndn

int main(int argc, char* argv[]) {
  return ndn::vsync::examples::main(argc, argv);
}
