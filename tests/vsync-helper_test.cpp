/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include "vsync-helper.hpp"

BOOST_AUTO_TEST_SUITE(TestVsyncHelper);

using namespace ndn::vsync;

BOOST_AUTO_TEST_CASE(MergeVV) {
  VersionVector v1{1, 0, 5};
  VersionVector v2{2, 4, 1};
  auto r1 = ndn::vsync::Merge(v1, v2);
  BOOST_TEST(r1 == VersionVector({2, 4, 5}), boost::test_tools::per_element());

  VersionVector v3{1, 0};
  auto r2 = ndn::vsync::Merge(v1, v3);
  BOOST_TEST(r2 == VersionVector());
}

BOOST_AUTO_TEST_CASE(VVEncodeDecode) {
  VersionVector v1{1, 5, 2, 4, 3};
  std::string out;
  EncodeVV(v1, out);
  VersionVector v2 = DecodeVV(out.data(), out.size());
  BOOST_TEST(v1 == v2, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(Names) {
  ViewID vid{1, "A"};
  VersionVector vv{1, 2, 1, 2};
  auto n1 = MakeVsyncInterestName(vid, vv);
  auto vid1 = ExtractViewID(n1);
  BOOST_TEST(vid1.first == vid.first);
  BOOST_TEST(vid1.second == vid.second);
  auto vv1 = ExtractVersionVector(n1);
  BOOST_TEST(vv1 == vv, boost::test_tools::per_element());

  uint8_t seq = 55;
  NodeID nid = "A";
  auto n2 = MakeDataName("/", nid, vid, seq);
  auto nid1 = ExtractNodeID(n2);
  BOOST_TEST(nid == nid1);
  auto seq1 = ExtractSequenceNumber(n2);
  BOOST_TEST(seq == seq1);
}

BOOST_AUTO_TEST_CASE(ESNEncodeDecode) {
  ESN esn = {{0x12345678, "ABC"}, 0x1234};
  std::string buf;
  EncodeESN(esn, buf);

  auto p1 = DecodeESN(buf.data(), buf.size());
  BOOST_TEST(p1.second == true);
  BOOST_CHECK_EQUAL(p1.first, esn);
}

BOOST_AUTO_TEST_SUITE_END();
