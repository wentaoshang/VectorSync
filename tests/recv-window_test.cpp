/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include <string>

#include "recv-window.hpp"
#include "vsync-helper.hpp"

using ndn::vsync::ReceiveWindow;

BOOST_AUTO_TEST_SUITE(TestReceiveWindow);

BOOST_AUTO_TEST_CASE(CheckForMissingData) {
  ReceiveWindow rw;
  rw.Insert(1);
  rw.Insert(3);
  auto r = rw.CheckForMissingData(3);
  ReceiveWindow::SeqNumIntervalSet expected1;
  expected1.insert(ReceiveWindow::SeqNumInterval(2));
  BOOST_CHECK_EQUAL(r, expected1);

  rw.Insert(2);
  r = rw.CheckForMissingData(3);
  ReceiveWindow::SeqNumIntervalSet expected2;
  BOOST_CHECK_EQUAL(r, expected2);

  expected2.insert(ReceiveWindow::SeqNumInterval::closed(4, 50));
  r = rw.CheckForMissingData(50);
  BOOST_CHECK_EQUAL(r, expected2);

  rw.Insert(10);
  rw.Insert(11);
  r = rw.CheckForMissingData(50);
  ReceiveWindow::SeqNumIntervalSet expected3;
  expected3.insert(ReceiveWindow::SeqNumInterval::right_open(4, 10));
  expected3.insert(ReceiveWindow::SeqNumInterval::closed(12, 50));
  BOOST_CHECK_EQUAL(r, expected3);
}

BOOST_AUTO_TEST_CASE(HasAllDataBefore) {
  ReceiveWindow rw;
  rw.Insert(1);
  rw.Insert(3);
  BOOST_CHECK_EQUAL(rw.HasAllDataBefore(1), true);
  rw.Insert(2);
  BOOST_CHECK_EQUAL(rw.HasAllDataBefore(3), true);

  rw.Insert(4);
  rw.Insert(11);
  BOOST_CHECK_EQUAL(rw.HasAllDataBefore(3), true);
  BOOST_CHECK_EQUAL(rw.HasAllDataBefore(4), true);
  BOOST_CHECK_EQUAL(rw.HasAllDataBefore(5), false);
  BOOST_CHECK_EQUAL(rw.HasAllDataBefore(8), false);
}

BOOST_AUTO_TEST_CASE(Bounds) {
  ReceiveWindow rw;
  rw.Insert(1);
  rw.Insert(3);
  BOOST_CHECK_EQUAL(rw.LowerBound(), 1UL);
  BOOST_CHECK_EQUAL(rw.UpperBound(), 3UL);
  rw.Insert(2);
  BOOST_CHECK_EQUAL(rw.LowerBound(), 3UL);
  BOOST_CHECK_EQUAL(rw.UpperBound(), 3UL);
}

BOOST_AUTO_TEST_SUITE_END();
