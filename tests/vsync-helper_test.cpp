/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include <ndn-cxx/util/digest.hpp>

#include "vsync-helper.hpp"

BOOST_AUTO_TEST_SUITE(TestVsyncHelper);

using namespace ndn::vsync;

BOOST_AUTO_TEST_CASE(JoinVV) {
  VersionVector v1{1, 0, 5};
  VersionVector v2{2, 4, 1};
  auto r1 = ndn::vsync::Join(v1, v2);
  BOOST_TEST(r1 == VersionVector({2, 4, 5}), boost::test_tools::per_element());

  VersionVector v3{1, 0};
  auto r2 = ndn::vsync::Join(v1, v3);
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
  ndn::Name nid{"/test/A"};
  ViewID vid{1, nid};
  uint64_t seq = 55;

  auto n1 = MakeSyncInterestName(vid, nid, seq);
  BOOST_TEST(n1.size() == (kSyncPrefix.size() + 4 + nid.size() * 2));
  auto vid1 = ExtractViewID(n1);
  BOOST_TEST(vid1.view_num == vid.view_num);
  BOOST_TEST(vid1.leader_name == vid.leader_name);
  auto s1 = ExtractSequenceNumber(n1);
  BOOST_TEST(s1 == seq);

  auto n2 = MakeDataName(nid, seq);
  auto nid1 = ExtractNodeID(n2);
  BOOST_TEST(nid == nid1);
  auto seq1 = ExtractSequenceNumber(n2);
  BOOST_TEST(seq == seq1);

  auto n3 = MakeViewInfoName(vid);
  auto vid2 = ExtractViewID(n3);
  BOOST_TEST(vid2 == vid);
}

BOOST_AUTO_TEST_SUITE_END();
