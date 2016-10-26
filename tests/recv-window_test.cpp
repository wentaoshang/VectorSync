/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include <string>

#include "recv-window.hpp"

using ndn::vsync::ReceiveWindow;

BOOST_AUTO_TEST_SUITE(TestReceiveWindow);

BOOST_AUTO_TEST_CASE(Init) {
  ReceiveWindow rw;
  BOOST_CHECK_EQUAL(rw.Insert({{0, "A"}, 1}), false);
  BOOST_CHECK_EQUAL(rw.Insert({{1, ""}, 1}), false);
  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 0}), false);

  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 1}), true);
  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 3}), true);

  BOOST_CHECK_EQUAL(rw.Insert({{2, "B"}, 1}), true);

  BOOST_CHECK_EQUAL(rw.Insert({{2, "B1"}, 2}), false);
}

BOOST_AUTO_TEST_CASE(CheckForMissingData) {
  ReceiveWindow rw;
  rw.Insert({{1, "A"}, 1});
  rw.Insert({{1, "A"}, 3});
  auto intervals = rw.CheckForMissingData({{1, "A"}, 3}, {2, "A"});
  ReceiveWindow::SeqNumIntervalSet expected1;
  expected1.insert(ReceiveWindow::SeqNumInterval(2));
  BOOST_CHECK_EQUAL(intervals, expected1);

  // These checks should return {} due to failure
  ReceiveWindow::SeqNumIntervalSet expected2;
  intervals = rw.CheckForMissingData({{1, "A"}, 50}, {1, "A"});
  BOOST_CHECK_EQUAL(intervals, expected2);
  intervals = rw.CheckForMissingData({{1, "A"}, 3}, {2, "B"});
  BOOST_CHECK_EQUAL(intervals, expected2);
  intervals = rw.CheckForMissingData({{1, "A"}, 3}, {6, "A"});
  BOOST_CHECK_EQUAL(intervals, expected2);

  // These checks should return {} since the window is full
  rw.Insert({{1, "A"}, 2});
  intervals = rw.CheckForMissingData({{1, "A"}, 3}, {2, "A"});
  BOOST_CHECK_EQUAL(intervals, expected2);

  expected2.insert(ReceiveWindow::SeqNumInterval::closed(1, 50));
  intervals = rw.CheckForMissingData({{2, "A"}, 50}, {3, "A"});
  BOOST_CHECK_EQUAL(intervals, expected2);

  rw.Insert({{2, "A"}, 10});
  rw.Insert({{2, "A"}, 11});
  intervals = rw.CheckForMissingData({{2, "A"}, 50}, {3, "A"});
  ReceiveWindow::SeqNumIntervalSet expected3;
  expected3.insert(ReceiveWindow::SeqNumInterval::right_open(1, 10));
  expected3.insert(ReceiveWindow::SeqNumInterval::closed(12, 50));
  BOOST_CHECK_EQUAL(intervals, expected3);
}

BOOST_AUTO_TEST_SUITE_END();
