# 固定v0.2测试入口索引

自动从Git对象定位TEST/TEST_F/TEST_P及Python函数；源码是权威。C++按宏入口分段，断言数量是静态文本统计，非执行次数；参数化case不是展开后的CTest数量。Python包含helper与actor，不把每个函数当独立验收用例。机制分析见[测试章](07-tests.md)。

## tests/gateway/net/AsioConnectTest.cpp

[完整源码](source/tests/gateway/net/AsioConnectTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| AsioConnectTest.CopiesSendInputAndPreservesOrder | 94 | TEST_F | 3 | EXPECT_EQ(transport.pendingBytes(), first.size() + next.size()); / EXPECT_EQ(receivePeer(), "original bytes followed by bytes"); |
| AsioConnectTest.PausedInFlightReadIsDeliveredOnResume | 109 | TEST_F | 3 | EXPECT_TRUE(received.empty()); / EXPECT_EQ(received, "held bytes"); |
| AsioConnectTest.QueuedSendAndShutdownBeforeStartDrainAfterStart | 133 | TEST_F | 5 | EXPECT_EQ(connection->pendingBytes(), payload.size()); / EXPECT_EQ(receivePeer(), payload); |
| AsioConnectTest.ShutdownDrainsAndStillReceivesUntilPeerFin | 156 | TEST_F | 5 | EXPECT_EQ(receivePeer(), payload); / EXPECT_EQ(error, boost::asio::error::eof); |
| AsioConnectTest.PartialWritesDrainInOrderBeforeFin | 189 | TEST_F | 6 | ASSERT_EQ( / EXPECT_GT(connection->pendingBytes(), 0); |
| AsioConnectTest.PeerFinFlushesQueuedEchoBeforeClose | 227 | TEST_F | 2 | EXPECT_EQ(receivePeer(), "last message"); / EXPECT_FALSE(connection->connected()); |
| AsioConnectTest.WatermarksAndHardLimitBoundOutput | 239 | TEST_F | 6 | ASSERT_EQ( / EXPECT_EQ(messages, 0); |
| AsioConnectTest.OutputCallbackCanCloseAndReleaseOwner | 294 | TEST_F | 1 | EXPECT_TRUE(weak.expired()); |
| AsioConnectTest.ConcurrentSendsKeepOneOrderedWriteStream | 314 | TEST_F | 10 | ASSERT_EQ( / ASSERT_GT(connection->pendingBytes(), 0); |
| AsioConnectTest.ForeignQueuedSendDrainsBeforeShutdownFin | 406 | TEST_F | 4 | EXPECT_EQ(connection->pendingBytes(), payload.size()); / EXPECT_EQ(receivePeer(), payload); |
| AsioConnectTest.ConcurrentOutputLimitClosesOnce | 425 | TEST_F | 5 | EXPECT_LE( / EXPECT_FALSE(connection->connected()); |
| AsioConnectTest.CloseOnceDiscardsQueuedAndCancelledOperations | 469 | TEST_F | 8 | ASSERT_EQ( / ASSERT_GT(connection->pendingBytes(), 0); |
| AsioConnectTest.CancelledIoDoesNotRetainConnection | 519 | TEST_F | 1 | EXPECT_TRUE(weak.expired()); |
| AsioConnectTest.MessageCallbackCanCloseAndReleaseExternalOwner | 532 | TEST_F | 2 | EXPECT_EQ( / EXPECT_TRUE(weak.expired()); |
| AsioConnectTest.CallbackExceptionClosesOnlyThisConnection | 557 | TEST_F | 2 | EXPECT_NO_THROW(pump()); / EXPECT_FALSE(connection->connected()); |
| AsioConnectTest.ForeignThreadMutationIsRejected | 569 | TEST_F | 3 | EXPECT_EQ(connection->id(), id); / EXPECT_TRUE(rejected); |
| AsioConnectTest.IdentityIsUniqueAcrossBackends | 592 | TEST_F | 3 | ASSERT_EQ( / EXPECT_NE(native.id(), connection->id()); |
| AsioConnectTest.SocketIsNonblockingAndCloseOnExec | 612 | TEST_F | 2 | EXPECT_NE(::fcntl(connection->fd(), F_GETFL) & O_NONBLOCK, 0); / EXPECT_NE(::fcntl(connection->fd(), F_GETFD) & FD_CLOEXEC, 0); |

## tests/gateway/net/AsioTcpServerTest.cpp

[完整源码](source/tests/gateway/net/AsioTcpServerTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| AsioTcpServerTest.InterfaceStopJoinsWorkersAndPreservesExternalContext | 46 | TEST | 9 | EXPECT_EQ(server->maxConnections(), 8); / EXPECT_EQ(server->workerCount(), workers); |
| AsioTcpServerTest.VirtualDestructionReclaimsActiveAndQueuedConnections | 82 | TEST | 2 | EXPECT_EQ(server->connectionCount(), 32); / EXPECT_EQ(fdCount(), baseline); |
| AsioTcpServerTest.ImmediateStopHandlesRegistrationHandoff | 120 | TEST | 3 | ASSERT_EQ(server.connectionCount(), 1); / EXPECT_TRUE(server.stopped()); |
| AsioTcpServerTest.ForeignManagementThreadIsRejected | 150 | TEST | 2 | EXPECT_TRUE(rejected); / EXPECT_FALSE(server.stopped()); |
| AsioTcpServerTest.InvalidConfigurationAndBindFailureLeaveNoDescriptors | 178 | TEST | 4 | EXPECT_THROW( / EXPECT_THROW( |

## tests/gateway/net/AsioTcpServerTestHost.cpp

[完整源码](source/tests/gateway/net/AsioTcpServerTestHost.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| 无TEST宏或Python函数入口；这是宿主/支持代码 | — | — | — | — |

## tests/gateway/net/ChannelTest.cpp

[完整源码](source/tests/gateway/net/ChannelTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ChannelTest.StoresDescriptorAndInterestMasks | 12 | TEST | 3 | EXPECT_EQ(channel.fd(), 42); / EXPECT_EQ( |
| ChannelTest.DispatchesOnlyMatchingCallbacks | 26 | TEST | 8 | EXPECT_EQ(reads, 1); / EXPECT_EQ(writes, 0); |
| ChannelTest.HupDispatchesErrorCallback | 56 | TEST | 1 | EXPECT_EQ(errors, 1); |
| ChannelTest.MissingCallbacksAreSafe | 68 | TEST | 1 | EXPECT_NO_THROW(channel.handleEvent()); |
| ChannelTest.InterestHelpersPreserveMasksAndNotifyOnlyOnChange | 76 | TEST | 8 | EXPECT_EQ(channel.events(), static_cast<uint32_t>(EPOLLIN)); / EXPECT_EQ(updates, 3); |
| ChannelTest.FailedUpdateRestoresInterestMask | 106 | TEST | 3 | EXPECT_THROW(channel.enableWriting(), std::runtime_error); / EXPECT_EQ(channel.events(), static_cast<uint32_t>(EPOLLIN)); |
| ChannelTest.TiedOwnerSurvivesAllCallbacksInOneEvent | 120 | TEST | 3 | EXPECT_FALSE(weak.expired()); / EXPECT_EQ(reads, 1); |
| ChannelTest.ExpiredTieSuppressesEventCallbacks | 143 | TEST | 1 | EXPECT_EQ(callbacks, 0); |

## tests/gateway/net/ChaosRun.py

[完整源码](source/tests/gateway/net/ChaosRun.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| read_ws_message | 42 | def | 6 | assert not (first & 0x70), "unexpected RSV bits" |
| timestamp | 70 | def | 0 |  |
| Host.__init__ | 76 | def | 0 |  |
| Host.logs | 118 | def | 0 |  |
| Host.health | 144 | def | 2 | assert not self.errors, (self.name, self.errors) |
| Host.wait_ready | 149 | def | 1 | assert time.monotonic() < limit, (self.name, "startup timeout") |
| Host.count | 156 | def | 0 |  |
| Host.connect | 160 | def | 1 | assert reply.startswith(b"HTTP/1.1 101"), reply |
| Host.send | 174 | def | 0 |  |
| Host.receive | 180 | def | 0 |  |
| Host.expect_closed | 185 | def | 2 | assert code == 1008, (self.name, code) |
| Host.exchange | 203 | def | 1 | assert self.receive(peer) == expected, (self.name, "response mismatch") |
| Host.job | 225 | def | 4 | assert reply.startswith(b"HTTP/1.1 403"), reply |
| Host.wait_fd | 310 | def | 0 |  |
| Host.stop | 320 | def | 8 | assert not self.reader.is_alive(), (self.name, "log not drained") |
| Host.summary | 338 | def | 0 |  |
| Host.summary.quantile | 339 | def | 0 |  |
| Run.__init__ | 366 | def | 0 |  |
| Run.sample | 380 | def | 0 |  |
| Run.snapshot | 402 | def | 0 |  |
| Run.write_live | 415 | def | 0 |  |
| Run.telemetry | 420 | def | 0 |  |
| Run.surge | 429 | def | 0 |  |
| Run.execute | 446 | def | 3 | assert self.workload_seconds >= self.args.duration |
| main | 537 | def | 2 | assert args.duration >= 10 |

## tests/gateway/net/ChaosRunFramingTest.py

[完整源码](source/tests/gateway/net/ChaosRunFramingTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| run | 8 | def | 2 | assert reply.startswith(b"HTTP/1.1 101") |

## tests/gateway/net/ConnectionContextIntegrationTest.py

[完整源码](source/tests/gateway/net/ConnectionContextIntegrationTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| parse | 15 | def | 4 | assert len(fields) == 6, fields |
| command | 25 | def | 2 | assert opcode == 2 |
| check | 40 | def | 8 | assert state == 0 |
| check.connect | 41 | def | 1 | assert response.startswith(b"HTTP/1.1 101") |
| run | 61 | def | 1 | assert process.poll() is None and time.monotonic() < deadline, output |

## tests/gateway/net/ConnectionContextTest.cpp

[完整源码](source/tests/gateway/net/ConnectionContextTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ConnectionContextTest.PreservesAdmissionMetadataAndIsolatesSessions | 13 | TEST | 8 | EXPECT_EQ(first->connectionId, 42u); / EXPECT_EQ(first->peer.ip(), "127.0.0.2"); |
| ConnectionContextTest.LoginTransitionsAreTerminalAndMutuallyExclusive | 50 | TEST | 9 | EXPECT_EQ( / EXPECT_TRUE(authenticated.markAuthenticated()); |
| ConnectionContextTest.SessionAccessRequiresItsOwner | 77 | TEST | 2 | EXPECT_EQ(rejected, 3); / EXPECT_EQ( |
| ConnectionContextTest.CallbackCopiesRetainIgnoredContextUntilFinalRelease | 118 | TEST | 6 | EXPECT_EQ(std::this_thread::get_id(), owner); / EXPECT_EQ(info.get(), context.get()); |
| ConnectionContextTest.RejectsMissingContextAndInvalidFactoryResult | 150 | TEST | 4 | EXPECT_THROW( / EXPECT_THROW( |

## tests/gateway/net/ConnectionFeatureIntegrationTest.py

[完整源码](source/tests/gateway/net/ConnectionFeatureIntegrationTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| check | 17 | def | 5 | assert response.startswith(b"HTTP/1.1 101") |
| check.connect | 18 | def | 1 | assert response.startswith(b"HTTP/1.1 101") |
| check.send | 27 | def | 0 |  |
| check.echo | 33 | def | 2 | assert read_frame(peer) == (2, packet(payload, 2)) |
| check.closed | 39 | def | 2 | assert close_code(peer) == 1008 |
| run | 77 | def | 3 | assert "packet rate exceeded" in output and "packet type rejected" in output, output |

## tests/gateway/net/ConnectionFeatureTest.cpp

[完整源码](source/tests/gateway/net/ConnectionFeatureTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ConnectionFeatureTest.BucketBurstRefillCapacityAndDrop | 87 | TEST_F | 10 | EXPECT_EQ( / EXPECT_EQ( |
| ConnectionFeatureTest.BucketsArePerConnectionAndCloseOnExhaustion | 127 | TEST_F | 5 | EXPECT_EQ(first->inspectPacket({1, {}}), FeatureResult::Continue); / EXPECT_EQ(first->inspectPacket({1, {}}), FeatureResult::Close); |
| ConnectionFeatureTest.WhitelistRejectsDisallowedAndEmptyLists | 145 | TEST_F | 3 | EXPECT_EQ(first->inspectPacket({7, {}}), FeatureResult::Continue); / EXPECT_EQ(first->inspectPacket({2, {}}), FeatureResult::Close); |
| TokenBucketFeatureTest.RejectsNonfiniteAndNonlimitingConfiguration | 160 | TEST | 6 | EXPECT_THROW(TokenBucketFeature(0.5, 1), std::invalid_argument); / EXPECT_THROW(TokenBucketFeature(1, 0), std::invalid_argument); |
| ConnectionFeatureTest.OnlyCompleteFramesConsumeTokens | 190 | TEST_F | 6 | EXPECT_TRUE(transport->sent.empty()); / EXPECT_TRUE(runtime->connected()); |
| ConnectionFeatureTest.DroppedFrameDoesNotPreventNextCoalescedFrame | 213 | TEST_F | 2 | EXPECT_TRUE(runtime->connected()); / EXPECT_EQ( |
| ConnectionFeatureTest.WhitelistRejectsValidEnvelopeBeforeBusinessHandler | 247 | TEST_F | 3 | ASSERT_TRUE(payload); / EXPECT_FALSE(runtime->connected()); |
| ConnectionFeatureTest.RawBytesDoNotBecomePacketEvents | 268 | TEST_F | 1 | EXPECT_EQ(count.calls, 0); |
| ConnectionFeatureTest.OnClosedWaitsForActualTransportNotification | 287 | TEST_F | 3 | EXPECT_EQ(transport->closes, 1); / EXPECT_EQ(count.calls, 0); |

## tests/gateway/net/ConnectionLifecycleTest.cpp

[完整源码](source/tests/gateway/net/ConnectionLifecycleTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| TcpConnectionTest.ConcurrentSendsDrainOrderedRecordsOnOwner | 64 | TEST | 10 | ASSERT_GE(flags, 0); / ASSERT_EQ( |
| TcpConnectionTest.PeerFinWaitsForAcceptedForeignSend | 192 | TEST | 6 | ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0); / ASSERT_TRUE(connection->connected()); |
| TcpConnectionTest.ForeignOutputLimitClosesAndReleasesQueue | 220 | TEST | 4 | EXPECT_LE( / EXPECT_FALSE(connection->connected()); |
| TcpConnectionTest.CloseDropsForeignQueuedSendsAndWeakDrain | 250 | TEST | 5 | ASSERT_EQ(connection->pendingBytes(), payload.size()); / EXPECT_EQ(connection->pendingBytes(), 0); |
| TcpConnectionTest.ReadsAndEchoesIncomingBytes | 281 | TEST | 3 | ASSERT_EQ( / ASSERT_EQ( |
| TcpConnectionTest.ClosedPeerSendDoesNotRaiseSigpipe | 309 | TEST | 1 | EXPECT_EQ(closes, 1); |
| TcpConnectionTest.PeerEofNotifiesCloseCallbackOnce | 327 | TEST | 1 | EXPECT_EQ(closeCount, 1); |
| SocketTest.NonBlockingAcceptReturnsEmptyWhenNoConnectionIsReady | 346 | TEST | 1 | EXPECT_FALSE(listener.accept().has_value()); |
| SocketTest.AcceptedSocketIsNonBlockingAndCloseOnExec | 355 | TEST | 5 | ASSERT_EQ( / ASSERT_EQ( |
| TcpConnectionTest.EchoesBytesBeforePeerWriteShutdown | 388 | TEST | 4 | ASSERT_EQ( / ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0); |
| TcpConnectionTest.LocalWriteShutdownKeepsReceivingUntilPeerFin | 417 | TEST | 11 | ASSERT_EQ(::read(pair.peer.get(), buffer, sizeof(buffer)), 5); / EXPECT_EQ(std::string_view(buffer, 5), "reply"); |
| TcpConnectionTest.QueuedWriteShutdownWaitsForPeerFin | 456 | TEST | 12 | ASSERT_EQ( / ASSERT_GT(connection.pendingOutputBytes(), 0U); |
| TcpConnectionTest.LocalWriteShutdownDrainsBeforeFinAfterPeerFin | 534 | TEST | 9 | ASSERT_EQ( / ASSERT_GT(connection.pendingOutputBytes(), 0U); |
| AcceptorTest.DispatchesAcceptedSocketAndPeerAddress | 601 | TEST | 6 | ASSERT_EQ( / EXPECT_TRUE(accepted.valid()); |
| TcpConnectionTest.DrainsPendingOutputBeforeClosingAfterPeerShutdown | 652 | TEST | 8 | ASSERT_EQ( / ASSERT_NE(connection.channel().events() & EPOLLOUT, 0U); |
| TcpConnectionTest.HighWaterPausesReadsAndLowWaterResumesPendingEdge | 729 | TEST | 8 | ASSERT_EQ( / EXPECT_EQ( |
| TcpConnectionTest.SustainedHighWaterDisconnectsSlowClient | 793 | TEST | 7 | ASSERT_EQ( / EXPECT_TRUE(runtime->connected()); |
| TcpConnectionTest.HardOutputLimitDisconnectsImmediately | 852 | TEST | 2 | ASSERT_EQ( / EXPECT_EQ(closes, 1); |
| TcpConnectionTest.OutputEventsLeaveReadPolicyToComponents | 884 | TEST | 7 | ASSERT_EQ( / EXPECT_EQ(bytes, connection.pendingBytes()); |
| TcpConnectionTest.SlowPeerPendingOutputIsBounded | 931 | TEST | 4 | ASSERT_EQ( / EXPECT_LE( |
| TcpServerTest.StopWithoutConnectionsIsImmediateAndIdempotent | 971 | TEST | 5 | EXPECT_FALSE(server.stopped()); / EXPECT_EQ(server.maxConnections(), 4U); |
| TcpServerTest.StopFromAnotherThreadIsRejectedBeforeMutation | 988 | TEST | 3 | EXPECT_TRUE(rejected); / EXPECT_FALSE(server.stopped()); |

## tests/gateway/net/ConnectionOwnershipTest.cpp

[完整源码](source/tests/gateway/net/ConnectionOwnershipTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ConnectionOwnershipTest.CloseReleasesConnectionExactlyOnce | 140 | TEST_P | 12 | ASSERT_EQ(::setsockopt(pair.peer.get(), SOL_SOCKET, SO_LINGER, / ASSERT_EQ(::setsockopt(pair.peer.get(), SOL_SOCKET, SO_RCVTIMEO, |
| ConnectionCallbackTest.MessageFinishesAfterLastOwnerIsReleased | 210 | TEST | 12 | EXPECT_FALSE(tracked.weak.expired()); / EXPECT_EQ(tracked.destructions, 0); |
| ConnectionCallbackTest.ThrowingCallbackStillReleasesConnectionOnce | 249 | TEST | 6 | EXPECT_EQ(tracked.destructions, 0); / ASSERT_EQ(::send(pair.peer.get(), "x", 1, MSG_NOSIGNAL), 1); |
| TcpServerOwnershipTest.QueuedRemovalIsSafeAfterServerDestruction | 272 | TEST | 9 | ASSERT_EQ(::getsockname(reservation.get(), / ASSERT_EQ(::connect(peer.get(), address.sockaddrPtr(), |

## tests/gateway/net/ConnectionRuntimeTest.cpp

[完整源码](source/tests/gateway/net/ConnectionRuntimeTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ConnectionRuntimeTest.PauseReasonsDoNotResumeEachOther | 101 | TEST_F | 3 | EXPECT_TRUE(transport->paused); / EXPECT_FALSE(transport->paused); |
| ConnectionRuntimeTest.PacketShortCircuitAndDeferredCloseOnce | 112 | TEST_F | 9 | EXPECT_EQ(runtime->inspectPacket({1, {}}), FeatureResult::Drop); / EXPECT_EQ(second.packets, 0); |
| ConnectionRuntimeTest.RegistrationFreezesAtStart | 132 | TEST_F | 1 | EXPECT_THROW(runtime->features().add<Count>(), std::logic_error); |
| ConnectionRuntimeTest.CancelledTimerAndClosedRuntimeNeverInvokeFeature | 137 | TEST_F | 1 | EXPECT_EQ(fired, 0); |
| ConnectionRuntimeTest.ExceptionsCloseAndOutputReentryIsCoalesced | 158 | TEST_F | 3 | EXPECT_EQ(feature.calls, 2); / EXPECT_FALSE(runtime->connected()); |
| ConnectionRuntimeTest.CloseNotificationWaitsUntilFeatureReturns | 188 | TEST_F | 1 | EXPECT_FALSE(feature.notifiedInside); |
| ConnectionRuntimeTest.BackpressureHysteresisCancelsOldTimer | 218 | TEST_F | 7 | ASSERT_TRUE(transport->paused); / EXPECT_TRUE(transport->paused); |
| ConnectionRuntimeTest.CloseCancelsBackpressureTimer | 239 | TEST_F | 1 | EXPECT_EQ(transport->closes, 0); |
| BackpressureFeatureTest.RejectsInvalidConfiguration | 249 | TEST | 3 | EXPECT_THROW(BackpressureFeature(4, 4), std::invalid_argument); / EXPECT_THROW( |

## tests/gateway/net/EpollPollerPerformanceTest.cpp

[完整源码](source/tests/gateway/net/EpollPollerPerformanceTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| EpollPollerPerformanceTest.ReadyEventThroughput | 39 | TEST | 6 | ASSERT_EQ( / ASSERT_FALSE(active.empty()); |
| EpollPollerPerformanceTest.RegistrationChurn | 107 | TEST | 0 |  |

## tests/gateway/net/EpollPollerTest.cpp

[完整源码](source/tests/gateway/net/EpollPollerTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| EpollPollerTest.AddedChannelReportsReadableEvent | 80 | TEST | 4 | ASSERT_EQ(::write(fd.get(), &one, sizeof(one)), sizeof(one)); / ASSERT_EQ(active.size(), 1U); |
| EpollPollerTest.UpdateChangesInterestMask | 99 | TEST | 4 | EXPECT_TRUE(active.empty()); / ASSERT_EQ(active.size(), 1U); |
| EpollPollerTest.RemovedChannelIsNotReported | 120 | TEST | 2 | ASSERT_EQ(::write(fd.get(), &one, sizeof(one)), sizeof(one)); / EXPECT_TRUE(active.empty()); |
| EpollPollerTest.PollClearsPreviousResultsWhenNoEventsAreReady | 137 | TEST | 4 | ASSERT_EQ(::write(fd.get(), &one, sizeof(one)), sizeof(one)); / ASSERT_EQ(active.size(), 1U); |
| EpollPollerTest.ReportsMoreChannelsThanInitialEventBatch | 158 | TEST | 5 | ASSERT_EQ( / EXPECT_NE(channel->revents() & EPOLLIN, 0U); |
| EpollPollerTest.InvalidDescriptorIsRejected | 204 | TEST | 1 | EXPECT_THROW(poller.addChannel(&channel), std::system_error); |
| EpollPollerTest.InterruptedWaitClearsPreviousResults | 213 | TEST | 1 | EXPECT_TRUE(active.empty()); |

## tests/gateway/net/EventLoopTest.cpp

[完整源码](source/tests/gateway/net/EventLoopTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| EventLoopTest.RunInLoopKeepsCallbacksOnTheOwnerThread | 22 | TEST | 4 | EXPECT_TRUE(ranInline); / EXPECT_FALSE(ranFromOtherThread.load()); |
| EventLoopTest.FourThreadsDeliverFourHundredThousandTasksAndStop | 55 | TEST | 6 | EXPECT_FALSE(timedOut); / EXPECT_FALSE(wrongThread.load()); |
| EventLoopTest.QuitFromAnotherThreadWakesIdlePoll | 149 | TEST | 0 |  |
| EventLoopTest.RemovingReadyChannelSkipsItsPendingEvent | 168 | TEST | 5 | ASSERT_TRUE(firstFd.valid()); / ASSERT_TRUE(secondFd.valid()); |

## tests/gateway/net/EventLoopThreadPoolTest.cpp

[完整源码](source/tests/gateway/net/EventLoopThreadPoolTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| EventLoopThreadPoolTest.RejectsNullBaseLoop | 97 | TEST | 1 | EXPECT_THROW( |
| EventLoopThreadPoolTest.ZeroWorkersUseBaseWithoutStoppingIt | 105 | TEST | 11 | EXPECT_FALSE(pool.started()); / EXPECT_THROW(pool.getNextLoop(), std::logic_error); |
| EventLoopThreadPoolTest.RoundRobinUsesDistinctWorkerOwners | 135 | TEST | 12 | ASSERT_EQ(loops.size(), 3u); / EXPECT_NE(loops[0], loops[1]); |
| EventLoopThreadPoolTest.RestartAndDestructionDoNotLeakFds | 185 | TEST | 4 | ASSERT_EQ(pool.getAllLoops().size(), 3u); / EXPECT_FALSE(pool.started()); |
| EventLoopThreadPoolTest.FailedStartupRollsBackAndCanRetry | 205 | TEST | 7 | EXPECT_THROW(pool.start(), std::system_error); / EXPECT_FALSE(pool.started()); |
| EventLoopThreadPoolTest.ManagementRejectsForeignThread | 225 | TEST | 2 | EXPECT_EQ(rejected, 6); / EXPECT_TRUE(pool.started()); |
| EventLoopThreadPoolTest.WorkerCannotStopItsOwnPool | 260 | TEST | 2 | ASSERT_EQ(result.wait_for(3s), std::future_status::ready); / EXPECT_TRUE(result.get()); |
| EventLoopThreadPoolTest.StopsAllWorkersBeforeJoiningFirst | 286 | TEST | 3 | ASSERT_EQ(ready.wait_for(3s), std::future_status::ready); / ASSERT_EQ(entered.wait_for(3s), std::future_status::ready); |
| EventLoopThreadPoolTest.WorkerErrorStillJoinsOtherWorkers | 319 | TEST | 9 | ASSERT_EQ(ready.wait_for(3s), std::future_status::ready); / ASSERT_EQ(entered.wait_for(3s), std::future_status::ready); |
| EventLoopThreadPoolTest.MultipleProducersRunOnSelectedWorkers | 375 | TEST | 3 | ASSERT_EQ(result.wait_for(3s), std::future_status::ready); / EXPECT_EQ(count.load(), tasksPerWorker); |
| EventLoopThreadPoolDeathTest.DestructorHandlesWorkerException | 432 | TEST | 1 | EXPECT_EXIT( |
| EventLoopThreadPoolDeathTest.WorkerDestructionTerminatesDirectly | 460 | TEST | 1 | EXPECT_EXIT( |

## tests/gateway/net/EventLoopThreadTest.cpp

[完整源码](source/tests/gateway/net/EventLoopThreadTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| EventLoopThreadTest.UnstartedObjectCanBeDestroyed | 75 | TEST | 0 |  |
| EventLoopThreadTest.RepeatedStartupAndDestructionWakeIdlePoll | 80 | TEST | 0 |  |
| EventLoopThreadTest.ReturnedLoopRunsTasksOnWorkerAndStopJoins | 90 | TEST | 7 | ASSERT_NE(loop, nullptr); / EXPECT_EQ(thread.startLoop(), loop); |
| EventLoopThreadTest.LoopCanExitBeforeWrapperDestruction | 111 | TEST | 1 | ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), |
| EventLoopThreadTest.CallbackExceptionIsReportedByStop | 131 | TEST | 2 | ASSERT_EQ(started.wait_for(std::chrono::seconds(3)), / EXPECT_THROW(thread.stop(), std::runtime_error); |
| EventLoopThreadTest.DestructorHandlesWorkerException | 147 | TEST | 1 | ASSERT_EQ(started.wait_for(std::chrono::seconds(3)), |
| EventLoopThreadTest.StopWaitsForCurrentCallback | 163 | TEST | 2 | ASSERT_EQ(started.wait_for(std::chrono::seconds(3)), / EXPECT_TRUE(returned); |
| EventLoopThreadTest.WorkerRejectsJoiningItself | 189 | TEST | 3 | ASSERT_EQ(result.wait_for(std::chrono::seconds(3)), / EXPECT_TRUE(result.get()); |
| EventLoopThreadDeathTest.ConstructionFailureReachesStartingThread | 213 | TEST | 1 | EXPECT_EXIT( |
| EventLoopThreadTest.StopWaitsForStartupBeforeJoining | 238 | TEST | 6 | EXPECT_TRUE(EventLoopThreadTestPeer::waitForStop(thread)); / EXPECT_EQ(done.wait_for(std::chrono::milliseconds(50)), |
| EventLoopThreadTest.ConcurrentStopsJoinOnlyOnce | 298 | TEST | 2 | EXPECT_EQ(failures.load(), 0); / EXPECT_THROW(thread.startLoop(), std::logic_error); |
| EventLoopThreadDeathTest.WorkerDestructionTerminatesWithoutThrowing | 329 | TEST | 1 | EXPECT_EXIT( |
| EventLoopThreadDeathTest.WorkerStopRejectsWhileAnotherCallerJoins | 353 | TEST | 1 | EXPECT_EXIT( |
| EventLoopThreadTest.WorkerCanRequestStopWithoutJoining | 392 | TEST | 3 | ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), / EXPECT_NO_THROW(thread.stop()); |
| EventLoopThreadTest.UnstartedStopRequestAllowsLaterStartup | 409 | TEST | 3 | EXPECT_NO_THROW(thread.requestStop()); / EXPECT_NE(thread.startLoop(), nullptr); |

## tests/gateway/net/IConnectionTest.cpp

[完整源码](source/tests/gateway/net/IConnectionTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| IConnectionTest.ReportsClosedStateAndClosesOnlyOnce | 73 | TEST_F | 7 | EXPECT_TRUE(connection->connected()); / EXPECT_NE(id, 0U); |
| IConnectionTest.PauseAndResumeDeliverAlreadyAvailableBytes | 90 | TEST_F | 6 | ASSERT_EQ(concrete->channel().events() & EPOLLIN, 0U); / ASSERT_EQ(::write(pair.peer.get(), "held", 4), 4); |
| IConnectionTest.HalfClosedConnectionKeepsReceivingUntilPeerFin | 110 | TEST_F | 10 | EXPECT_TRUE(connection->connected()); / ASSERT_EQ(::read(pair.peer.get(), buffer, sizeof(buffer)), 5); |
| IConnectionTest.QueuedSendOwnsBytesUntilDrainedBeforeFin | 135 | TEST_F | 12 | ASSERT_EQ(::setsockopt(concrete->fd(), SOL_SOCKET, SO_SNDBUF, / ASSERT_GT(connection->pendingBytes(), 0U); |
| IConnectionTest.IdentityIsStableAndNotReusedAfterDestruction | 186 | TEST_F | 4 | EXPECT_NE(firstId, secondId); / EXPECT_NE(second->id(), firstId); |
| IConnectionOwnershipTest.VirtualDestructionOnOwnerClosesSocket | 206 | TEST | 1 | EXPECT_EQ(::read(pair.peer.get(), &byte, 1), 0); |

## tests/gateway/net/MessageDispatchIntegrationTest.py

[完整源码](source/tests/gateway/net/MessageDispatchIntegrationTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| envelope | 18 | def | 0 |  |
| fields | 23 | def | 3 | assert offset < len(data) and shift < 70 |
| fields.number | 25 | def | 1 | assert offset < len(data) and shift < 70 |
| response | 51 | def | 3 | assert magic == 0x4152454E and size == len(data) - 8 |
| run | 63 | def | 4 | assert len(starts) == 2 and sorted(starts) == sorted(ends), output |
| run_profile | 101 | def | 9 | assert "do-not-log-private-business-data" not in logs, logs |

## tests/gateway/net/MessageDispatchTest.cpp

[完整源码](source/tests/gateway/net/MessageDispatchTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| MessageDispatchTest.UnknownMessageReturnsStableTypedErrorAndRequestId | 142 | TEST_F | 6 | EXPECT_EQ(reply.msg_id(), wire::MSG_ERROR); / EXPECT_EQ(reply.request_id(), 1234567890123ULL); |
| MessageDispatchTest.TypedBusinessExceptionMapsToStableError | 161 | TEST_F | 5 | EXPECT_EQ(reply.request_id(), 77); / ASSERT_TRUE(reply.has_error()); |
| MessageDispatchTest.TimeoutRepliesOnceAndLateCompletionIsIgnored | 183 | TEST_F | 8 | EXPECT_TRUE(transport->output.empty()); / ASSERT_EQ(deadlines.size(), 1); |
| MessageDispatchTest.MalformedBusinessReplyBecomesInternalError | 215 | TEST_F | 4 | ASSERT_TRUE(reply.has_error()); / EXPECT_EQ(reply.error().code(), 9000); |
| MessageDispatchTest.ResponseRespectsOuterPacketBudget | 238 | TEST_F | 3 | ASSERT_TRUE(reply.has_error()); / EXPECT_EQ(reply.error().code(), wire::ERR_INTERNAL); |
| MessageDispatchTest.LegacyRequestIdFallbackAndSpdlogTraceStayConsistent | 272 | TEST_F | 10 | EXPECT_EQ(request->requestId, 7); / EXPECT_EQ(request->context->connectionId, 1); |
| MessageDispatchTest.AuthenticationIsCheckedBeforeRegisteredBusinessHandler | 331 | TEST_F | 6 | EXPECT_EQ(response().error().code(), 1003); / EXPECT_EQ(calls, 0); |
| MessageDispatchTest.MismatchedMsgIdAndPayloadNeverInvokeHandler | 363 | TEST_F | 2 | EXPECT_EQ(response().error().code(), 1002); / EXPECT_EQ(calls, 0); |
| MessageDispatchTest.OldCompletionAndTimerCannotFinishReusedRequestId | 383 | TEST_F | 3 | EXPECT_EQ(response().error().code(), 1004); / EXPECT_TRUE(transport->output.empty()); |
| MessageDispatchTest.DuplicateActiveRequestTerminatesOriginalWithOneError | 414 | TEST_F | 3 | EXPECT_EQ(response().error().code(), 1006); / EXPECT_EQ(calls, 1); |
| MessageDispatchTest.PendingRequestsHaveAnExplicitBound | 444 | TEST_F | 2 | EXPECT_EQ(response().error().code(), 1005); / EXPECT_EQ(deadlines.size(), 64); |
| MessageDispatchTest.DisconnectCancelsTimersAndLateCallbacksOwnNoConnection | 464 | TEST_F | 4 | EXPECT_TRUE(weak.expired()); / EXPECT_EQ(cancellations, 1); |
| MessageDispatchTest.UntypedExceptionsAndInvalidTypedCodesMapToInternal | 502 | TEST_F | 3 | EXPECT_EQ(response().error().code(), 9000); / EXPECT_EQ( |
| MessageRegistryTest.RejectsDuplicateMissingInvalidAndLateRegistrations | 531 | TEST | 6 | EXPECT_THROW( / EXPECT_THROW( |

## tests/gateway/net/PacketBufferTest.cpp

[完整源码](source/tests/gateway/net/PacketBufferTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| PacketBufferTest.InitialState | 12 | TEST | 2 | EXPECT_EQ(buffer.readableBytes(), 0); / EXPECT_GT(buffer.writableBytes(), 0); |
| PacketBufferTest.AppendData | 20 | TEST | 3 | EXPECT_TRUE(buffer.append(data, 5)); / EXPECT_EQ(buffer.readableBytes(), 5); |
| PacketBufferTest.SkipData | 36 | TEST | 4 | ASSERT_TRUE(buffer.append(data, 5)); / EXPECT_TRUE(buffer.skip(2)); |
| PacketBufferTest.SkipAllDataResetsBuffer | 54 | TEST | 6 | ASSERT_TRUE(buffer.append(data, 5)); / ASSERT_TRUE(buffer.skip(5)); |
| PacketBufferTest.SkipTooMuchFails | 78 | TEST | 4 | ASSERT_TRUE(buffer.append(data, 5)); / EXPECT_FALSE(buffer.skip(6)); |
| PacketBufferTest.NullptrWithNonZeroLengthFails | 97 | TEST | 2 | EXPECT_FALSE(buffer.append(nullptr, 10)); / EXPECT_EQ(buffer.readableBytes(), 0); |
| PacketBufferTest.NullptrWithZeroLengthIsAllowed | 106 | TEST | 2 | EXPECT_TRUE(buffer.append(nullptr, 0)); / EXPECT_EQ(buffer.readableBytes(), 0); |
| PacketBufferTest.ResizeWhenCapacityIsInsufficient | 115 | TEST | 3 | ASSERT_TRUE(buffer.append(data, 10)); / EXPECT_EQ(buffer.readableBytes(), 10); |
| PacketBufferTest.AppendFailsWhenExceedingMaxCapacity | 131 | TEST | 4 | ASSERT_TRUE(buffer.append(first, 4)); / EXPECT_FALSE(buffer.append(second, 5)); |
| PacketBufferTest.CompactConsumedSpaceBeforeResize | 151 | TEST | 5 | ASSERT_TRUE(buffer.append(first, 6)); / ASSERT_TRUE(buffer.skip(4)); |
| PacketBufferTest.MultipleAppendMaintainsOrder | 199 | TEST | 5 | ASSERT_TRUE(buffer.append(first, 3)); / ASSERT_TRUE(buffer.append(second, 3)); |
| PacketBufferTest.AppendAfterPartialSkip | 223 | TEST | 5 | ASSERT_TRUE(buffer.append("abcdef", 6)); / ASSERT_TRUE(buffer.skip(3)); |
| PacketBufferTest.SharesBaseStorageAndLegacyReadNames | 244 | TEST | 8 | EXPECT_EQ(base.prependableBytes(), 0); / ASSERT_TRUE(base.append("abcd", 4)); |
| PacketBufferTest.DefaultCapacityRemainsOneMiB | 261 | TEST | 4 | ASSERT_TRUE(buffer.append(data.data(), data.size())); / EXPECT_EQ( |

## tests/gateway/net/PacketCodecIntegrationTest.py

[完整源码](source/tests/gateway/net/PacketCodecIntegrationTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| packet | 21 | def | 0 |  |
| receive_exact | 24 | def | 0 |  |
| expect_closed | 33 | def | 0 |  |
| run | 40 | def | 14 | assert "TcpServer stopped" in diagnostics |

## tests/gateway/net/PacketPipelineTest.cpp

[完整源码](source/tests/gateway/net/PacketPipelineTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| PacketPipelineTest.ForwardsInOrderAndStopsWhenHandled | 132 | TEST | 3 | EXPECT_EQ(packet.payload, "payload"); / EXPECT_EQ(pipeline.process(context), PacketHandleResult::Handled); |
| PacketPipelineTest.RejectStopsBeforeDownstreamHandler | 167 | TEST | 2 | EXPECT_EQ(pipeline.process(context), PacketHandleResult::Reject); / EXPECT_EQ(downstream, 0); |
| PacketPipelineTest.UnhandledFrameIsRejectedAndEmptyHeadIsInvalid | 189 | TEST | 3 | EXPECT_THROW((PacketPipeline(nullptr)), std::invalid_argument); / EXPECT_FALSE(connection.connected()); |
| PacketPipelineTest.EnvelopeDecoderFeedsNextHandlerAndRejectsMalformed | 206 | TEST | 8 | ASSERT_TRUE(wire); / EXPECT_TRUE(packet.envelope.has_value()); |
| PacketPipelineTest.HandlerRunsOnlyForCompleteFramesAndClosesOnReject | 246 | TEST | 7 | EXPECT_EQ(calls, 0); / EXPECT_TRUE(connection.connected()); |
| PacketPipelineTest.FreshChainsIsolateStateAndCallbackCopiesRetainNodes | 279 | TEST | 7 | EXPECT_TRUE(connection1.connected()); / EXPECT_TRUE(connection2.connected()); |
| PacketPipelineTest.HandlerExceptionClosesAndDiscardsRemainingFrames | 314 | TEST | 3 | EXPECT_EQ(calls, 1); / EXPECT_EQ(connection.closes, 1); |
| PacketPipelineTest.PingHandlerForwardsOtherEnvelopesToLaterRouter | 338 | TEST | 5 | ASSERT_TRUE(wire); / EXPECT_TRUE(context.envelope->has_login_request()); |

## tests/gateway/net/SecurityPolicyIntegrationTest.py

[完整源码](source/tests/gateway/net/SecurityPolicyIntegrationTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| login | 17 | def | 0 |  |
| check | 22 | def | 5 | assert response.startswith(b"HTTP/1.1 101") |
| check.connect | 23 | def | 1 | assert response.startswith(b"HTTP/1.1 101") |
| check.send | 32 | def | 0 |  |
| check.response | 38 | def | 2 | assert read_frame(peer) == (2, expected) |
| check.closed | 44 | def | 2 | assert opcode == 8 and payload[:2] == b"\x03\xf0", (opcode, payload) |
| run | 104 | def | 2 | assert match, output |

## tests/gateway/net/SecurityPolicyTest.cpp

[完整源码](source/tests/gateway/net/SecurityPolicyTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| SecurityPolicyTest.BurstStopsBeforeDecodeRouteAndRemainingCoalescedFrames | 142 | TEST_F | 7 | EXPECT_EQ(routed, 2); / EXPECT_FALSE(runtime->connected()); |
| SecurityPolicyTest.DropCountsEachRejectedPacketWithoutRoutingIt | 174 | TEST_F | 3 | EXPECT_EQ(routed, 1); / EXPECT_TRUE(runtime->connected()); |
| SecurityPolicyTest.PendingBusinessCannotReachRouter | 194 | TEST_F | 4 | EXPECT_EQ(routed, 0); / EXPECT_FALSE(runtime->connected()); |
| SecurityPolicyTest.PendingPingAndLoginDoNotAuthenticate | 204 | TEST_F | 4 | EXPECT_EQ(routed, 2); / EXPECT_EQ( |
| SecurityPolicyTest.HandshakePayloadCannotBypassGateWithBusinessOuterType | 224 | TEST_F | 2 | EXPECT_EQ(routed, 0); / EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1); |
| SecurityPolicyTest.AuthenticatedBusinessReachesRouter | 235 | TEST_F | 4 | ASSERT_TRUE(context->session->markAuthenticated()); / EXPECT_EQ(routed, 1); |
| SecurityPolicyTest.ExpiredSessionCannotRestartLoginOrPing | 245 | TEST_F | 4 | ASSERT_TRUE(context->session->expireLogin()); / EXPECT_FALSE(context->session->markAuthenticated()); |
| SecurityPolicyTest.WhitelistRejectionIsCountedOnceBeforeDecode | 255 | TEST_F | 3 | EXPECT_EQ(routed, 0); / EXPECT_EQ( |
| SecurityPolicyTest.MalformedEnvelopeCountsDecodeReasonOnly | 273 | TEST_F | 4 | EXPECT_EQ(routed, 0); / EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 1); |
| SecurityPolicyTest.MalformedFrameCountsFrameReasonOnly | 283 | TEST_F | 3 | EXPECT_EQ(routed, 0); / EXPECT_EQ(counters->count(RejectionReason::InvalidFrame), 1); |
| SecurityPolicyTest.TailRejectAndCallbackFailureHaveDistinctReasons | 294 | TEST_F | 3 | EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1); / EXPECT_EQ(counters->count(RejectionReason::CallbackFailure), 1); |
| SecurityPolicyConfigurationTest.GateAndCountersFailClosedOnMissingDependencies | 334 | TEST | 5 | EXPECT_THROW( / EXPECT_THROW( |
| RejectionCountersTest.SharedWorkersDoNotLoseCountsAndCountsOutliveConnection | 367 | TEST | 4 | EXPECT_EQ( / EXPECT_EQ(counters->snapshot()[0], 1); |

## tests/gateway/net/SessionManagerTest.cpp

[完整源码](source/tests/gateway/net/SessionManagerTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| SessionManagerTest.AddAndFindSession | 39 | TEST | 4 | ASSERT_EQ(::pipe(pipeFds), 0); / EXPECT_TRUE( |
| SessionManagerTest.FindMissingSessionReturnsNullptr | 64 | TEST | 1 | EXPECT_EQ(session, nullptr); |
| SessionManagerTest.DuplicateSessionIdIsRejected | 75 | TEST | 7 | ASSERT_EQ(::pipe(pipeA), 0); / ASSERT_EQ(::pipe(pipeB), 0); |
| SessionManagerTest.RemoveSession | 114 | TEST | 5 | ASSERT_EQ(::pipe(pipeFds), 0); / ASSERT_TRUE( |
| SessionManagerTest.RemoveSessionClosesDescriptor | 140 | TEST | 4 | ASSERT_EQ(::pipe(pipeFds), 0); / ASSERT_TRUE( |
| SessionManagerTest.DestructorReleasesAllSessions | 166 | TEST | 8 | ASSERT_EQ(::pipe(pipeA), 0); / ASSERT_EQ(::pipe(pipeB), 0); |

## tests/gateway/net/SocketTest.cpp

[完整源码](source/tests/gateway/net/SocketTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| SocketTest.CreateTcpSocket | 18 | TEST | 4 | EXPECT_TRUE(socket.valid()); / EXPECT_GE(socket.get(), 0); |
| SocketTest.CreateTcpNonBlocking | 43 | TEST | 3 | ASSERT_TRUE(socket.valid()); / ASSERT_NE(flags, -1); |
| SocketTest.CreateTcpNonBlockingHasCloseOnExec | 56 | TEST | 3 | ASSERT_TRUE(socket.valid()); / ASSERT_NE(flags, -1); |
| SocketTest.BindToLoopback | 69 | TEST | 4 | EXPECT_NO_THROW( / ASSERT_EQ( |
| SocketTest.Listen | 97 | TEST | 3 | EXPECT_NO_THROW( / ASSERT_EQ( |
| SocketTest.AcceptConnection | 126 | TEST | 7 | ASSERT_EQ( / ASSERT_GE(fd, 0); |
| SocketTest.DestructorClosesDescriptor | 195 | TEST | 4 | ASSERT_GE(fd, 0); / EXPECT_NE( |

## tests/gateway/net/TcpServerChurnMonitor.py

[完整源码](source/tests/gateway/net/TcpServerChurnMonitor.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| percentile | 31 | def | 0 |  |
| ticks | 38 | def | 0 |  |
| proc_metrics | 43 | def | 0 |  |
| Monitor.__init__ | 80 | def | 0 |  |
| Monitor.event | 114 | def | 0 |  |
| Monitor.snapshot | 123 | def | 0 |  |
| Monitor.write_snapshot | 139 | def | 0 |  |
| Monitor.read_logs | 144 | def | 0 |  |
| Monitor.sample | 176 | def | 0 |  |
| Monitor.sample_loop | 222 | def | 0 |  |
| Monitor.start_http | 229 | def | 0 |  |
| Monitor.start_http.Handler.do_GET | 234 | def | 0 |  |
| Monitor.start_http.Handler.log_message | 251 | def | 0 |  |
| Monitor.wait_fd | 260 | async def | 0 |  |
| Monitor.check_health | 270 | def | 0 |  |
| Monitor.churn_round | 276 | async def | 0 |  |
| Monitor.churn_round.open_peer | 286 | async def | 0 |  |
| Monitor.churn_round.echo_peer | 297 | async def | 0 |  |
| Monitor.churn_round.close_peer | 311 | async def | 0 |  |
| Monitor.churn_round.settle | 335 | async def | 0 |  |
| Monitor.workload | 369 | async def | 0 |  |
| Monitor.write_report | 454 | def | 0 |  |
| Monitor.run | 493 | def | 0 |  |
| main | 511 | def | 0 |  |

## tests/gateway/net/TcpServerIntegrationTest.py

[完整源码](source/tests/gateway/net/TcpServerIntegrationTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| wait_until | 19 | def | 0 |  |
| fd_count | 32 | def | 0 |  |
| reserve_port | 36 | def | 0 |  |
| connect | 42 | def | 0 |  |
| run | 48 | def | 0 |  |
| run.constrain_fds | 62 | def | 0 |  |

## tests/gateway/net/TcpServerTestHost.cpp

[完整源码](source/tests/gateway/net/TcpServerTestHost.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| 无TEST宏或Python函数入口；这是宿主/支持代码 | — | — | — | — |

## tests/gateway/net/TcpServerWorkerTest.cpp

[完整源码](source/tests/gateway/net/TcpServerWorkerTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| TcpServerWorkerTest.StopsAndJoinsIdlePoolBeforeReportingStopped | 99 | TEST | 9 | EXPECT_EQ(server.workerCount(), 4u); / EXPECT_EQ(server.maxConnections(), 16u); |
| TcpServerWorkerTest.ActiveDestructionReclaimsSocketsAndSkipsLateNotices | 122 | TEST | 5 | EXPECT_EQ(base.pendingTimerCount(), 1u); / EXPECT_FALSE(timedOut); |
| TcpServerWorkerTest.GracefulStopFansOutFinAndReclaimsDeadlinePeers | 188 | TEST | 5 | EXPECT_FALSE(timedOut); / EXPECT_TRUE(requested); |
| TcpServerWorkerTest.ForeignRegistrationAndConstructionAreRejected | 260 | TEST | 4 | EXPECT_THROW(owner->addChannel(nullptr), std::logic_error); / EXPECT_THROW(owner->updateChannel(nullptr), std::logic_error); |
| TcpConnectionOwnershipDeathTest.ForeignFinalReleaseTerminatesWithoutThrow | 278 | TEST | 1 | EXPECT_EXIT( |
| ITcpServerTest.NegativeGraceDoesNotChangeRunningState | 305 | TEST | 6 | EXPECT_EQ(server->workerCount(), 0u); / EXPECT_THROW(server->stop(-1ms), std::invalid_argument); |

## tests/gateway/net/TimerQueueTest.cpp

[完整源码](source/tests/gateway/net/TimerQueueTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| TimerQueueTest.OneShotTimersRunInDeadlineOrder | 19 | TEST | 2 | EXPECT_EQ(fired, (std::vector<int>{1, 2, 3})); / EXPECT_EQ(loop.pendingTimerCount(), 0U); |
| TimerQueueTest.CancelEarliestAndRepeatingTimer | 34 | TEST | 6 | EXPECT_TRUE(loop.cancelTimer(canceled)); / EXPECT_FALSE(loop.cancelTimer(canceled)); |
| TimerQueueTest.CallbackCanAddAndCancelOtherTimers | 67 | TEST | 2 | EXPECT_TRUE(loop.cancelTimer(later)); / EXPECT_EQ(fired, (std::vector<int>{1, 2})); |
| TimerQueueTest.RejectsInvalidInputAndForeignThreadMutation | 91 | TEST | 4 | EXPECT_THROW(loop.runAfter(0ms, [] {}), std::invalid_argument); / EXPECT_THROW(loop.runEvery(-1ms, [] {}), std::invalid_argument); |
| TimerQueueTest.CancelManyTimersKeepsHeapIndexValid | 115 | TEST | 3 | EXPECT_TRUE(loop.cancelTimer(ids[i])); / EXPECT_EQ(executions[i], i % 3 == 0 ? 0 : 1) << i; |
| TimerQueueTest.HundredThousandTimersFireOrCancelExactlyOnce | 143 | TEST | 8 | ASSERT_TRUE(loop.cancelTimer(ids[i])) << i; / EXPECT_EQ(loop.pendingTimerCount(), expectedFires); |
| TimerQueueTest.DestructionDropsPendingCallbacks | 232 | TEST | 5 | ASSERT_EQ(repeatedCalls->load(), 1); / ASSERT_EQ(oneShotCalls->load(), 0); |

## tests/gateway/net/WebSocketIntegrationTest.py

[完整源码](source/tests/gateway/net/WebSocketIntegrationTest.py.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| exact | 20 | def | 0 |  |
| varint | 30 | def | 0 |  |
| envelope_ping | 39 | def | 0 |  |
| packet | 44 | def | 0 |  |
| packet_pong | 48 | def | 0 |  |
| read_tcp_packet | 53 | def | 0 |  |
| upgrade | 59 | def | 1 | assert len(reply) < 8192 |
| send_frame | 77 | def | 0 |  |
| read_frame | 94 | def | 2 | assert not second & 0x80, "server frame must be unmasked" |
| close_code | 106 | def | 1 | assert len(payload) >= 2 |
| run_host | 119 | def | 0 |  |
| run_host.content | 128 | def | 0 |  |
| stop_host | 140 | def | 1 | assert process.wait(timeout=8) == 0 |
| run | 152 | def | 14 | assert "code=1003" in output and "code=4001" in output, output |

## tests/gateway/net/WebSocketTestHost.cpp

[完整源码](source/tests/gateway/net/WebSocketTestHost.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| 无TEST宏或Python函数入口；这是宿主/支持代码 | — | — | — | — |

## tests/shared/concurrent/BlockingQueuePerformanceTest.cpp

[完整源码](source/tests/shared/concurrent/BlockingQueuePerformanceTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| BlockingQueuePerformanceTest.SingleProducerSingleConsumer | 23 | TEST | 2 | ASSERT_TRUE(queue.push(i)); / EXPECT_EQ(checksum, expected); |
| BlockingQueuePerformanceTest.MultiProducerMultiConsumer | 82 | TEST | 1 | EXPECT_EQ(consumed.load(), total_items); |

## tests/shared/concurrent/BlockingQueueTest.cpp

[完整源码](source/tests/shared/concurrent/BlockingQueueTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| BlockingQueueTest.InitialState | 24 | TEST | 5 | EXPECT_EQ(queue.capacity(), 4); / EXPECT_EQ(queue.size(), 0); |
| BlockingQueueTest.ZeroCapacityThrows | 36 | TEST | 1 | EXPECT_THROW( |
| BlockingQueueTest.PushAndPopSingleElement | 45 | TEST | 7 | EXPECT_TRUE(queue.push(42)); / EXPECT_EQ(queue.size(), 1); |
| BlockingQueueTest.FIFOOrder | 64 | TEST | 9 | EXPECT_TRUE(queue.push(1)); / EXPECT_TRUE(queue.push(2)); |
| BlockingQueueTest.FullState | 86 | TEST | 6 | EXPECT_TRUE(queue.push(1)); / EXPECT_TRUE(queue.push(2)); |
| BlockingQueueTest.SupportsMoveOnlyType | 103 | TEST | 5 | EXPECT_TRUE(queue.push(std::move(ptr))); / EXPECT_EQ(ptr, nullptr); |
| BlockingQueueTest.CloseChangesState | 125 | TEST | 2 | EXPECT_FALSE(queue.closed()); / EXPECT_TRUE(queue.closed()); |
| BlockingQueueTest.CloseIsIdempotent | 137 | TEST | 1 | EXPECT_TRUE(queue.closed()); |
| BlockingQueueTest.PushFailsAfterClose | 149 | TEST | 2 | EXPECT_FALSE(queue.push(1)); / EXPECT_EQ(queue.size(), 0); |
| BlockingQueueTest.ExistingElementsRemainAvailableAfterClose | 160 | TEST | 7 | EXPECT_TRUE(queue.push(10)); / EXPECT_TRUE(queue.push(20)); |
| BlockingQueueTest.PopReturnsNulloptWhenClosedAndEmpty | 183 | TEST | 1 | EXPECT_FALSE(result.has_value()); |
| BlockingQueueTest.ConsumerBlocksUntilPush | 199 | TEST | 4 | EXPECT_FALSE(consumer_finished.load()); / EXPECT_TRUE(queue.push(123)); |
| BlockingQueueTest.ProducerBlocksWhenFull | 237 | TEST | 7 | ASSERT_TRUE(queue.push(1)); / EXPECT_FALSE(producer_finished.load()); |
| BlockingQueueTest.CloseWakesBlockedConsumer | 278 | TEST | 3 | EXPECT_FALSE(value.has_value()); / EXPECT_FALSE(finished.load()); |
| BlockingQueueTest.CloseWakesBlockedProducer | 304 | TEST | 4 | ASSERT_TRUE(queue.push(1)); / EXPECT_FALSE(finished.load()); |
| BlockingQueueTest.MultipleProducersMultipleConsumers | 335 | TEST | 3 | ASSERT_TRUE(queue.push(value)); / EXPECT_EQ(consumed_count.load(), total_items); |
| BlockingQueueTest.NoLostOrDuplicatedItems | 422 | TEST | 2 | ASSERT_TRUE(queue.push(value)); / EXPECT_EQ( |

## tests/shared/concurrent/ThreadPoolPerformanceTest.cpp

[完整源码](source/tests/shared/concurrent/ThreadPoolPerformanceTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ThreadPoolPerformanceTest.MandelbrotBenchmark | 249 | TEST | 1 | EXPECT_EQ( |

## tests/shared/concurrent/ThreadPoolTest.cpp

[完整源码](source/tests/shared/concurrent/ThreadPoolTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ThreadPoolTest.StartAndShutdown | 70 | TEST | 2 | EXPECT_NO_THROW(pool.start()); / EXPECT_NO_THROW(pool.shutdown()); |
| ThreadPoolTest.CannotStartTwice | 79 | TEST | 1 | EXPECT_THROW( |
| ThreadPoolTest.ShutdownBeforeStartIsSafe | 94 | TEST | 1 | EXPECT_NO_THROW( |
| ThreadPoolTest.CannotStartAfterShutdown | 104 | TEST | 1 | EXPECT_THROW( |
| ThreadPoolTest.MultipleShutdownIsSafe | 117 | TEST | 3 | EXPECT_NO_THROW(pool.shutdown()); / EXPECT_NO_THROW(pool.shutdown()); |
| ThreadPoolTest.RejectSubmitBeforeStart | 133 | TEST | 1 | EXPECT_THROW( |
| ThreadPoolTest.RejectSubmitAfterShutdown | 146 | TEST | 1 | EXPECT_THROW( |
| ThreadPoolTest.SubmitReturnsFutureResult | 162 | TEST | 1 | EXPECT_EQ( |
| ThreadPoolTest.SubmitVoidTask | 181 | TEST | 1 | EXPECT_EQ( |
| ThreadPoolTest.SubmitTaskWithArguments | 207 | TEST | 1 | EXPECT_EQ( |
| ThreadPoolTest.SubmitMoveOnlyArgument | 230 | TEST | 2 | EXPECT_EQ(future.get(), 42); / EXPECT_EQ(ptr, nullptr); |
| ThreadPoolTest.TaskExceptionPropagatesThroughFuture | 256 | TEST | 1 | EXPECT_THROW( |
| ThreadPoolTest.ExceptionDoesNotKillWorkerThread | 275 | TEST | 2 | EXPECT_THROW( / EXPECT_EQ( |
| ThreadPoolTest.MultipleTasksReturnCorrectResults | 307 | TEST | 1 | EXPECT_EQ( |
| ThreadPoolTest.GracefulShutdownCompletesQueuedTasks | 343 | TEST | 1 | EXPECT_EQ( |
| ThreadPoolTest.ShutdownWaitsForRunningTask | 374 | TEST | 2 | EXPECT_FALSE( / EXPECT_TRUE( |
| ThreadPoolTest.ConcurrentSubmit | 433 | TEST | 1 | EXPECT_EQ( |
| ThreadPoolTest.ConcurrentSubmitAndShutdownIsSafe | 486 | TEST | 2 | EXPECT_EQ( / EXPECT_THROW( |
| ThreadPoolTest.ConcurrentShutdownIsSafe | 556 | TEST | 2 | EXPECT_NO_THROW( / EXPECT_EQ( |
| ThreadPoolTest.DestructorPerformsGracefulShutdown | 607 | TEST | 1 | EXPECT_EQ( |
| ThreadPoolTest.WorkerHooksAreCalled | 642 | TEST | 4 | EXPECT_EQ( / EXPECT_EQ( |

## tests/shared/os/FileDescriptorTest.cpp

[完整源码](source/tests/shared/os/FileDescriptorTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| FileDescriptorTest.CopyDisabledMoveEnabled | 31 | TEST | 0 |  |
| FileDescriptorTest.InvalidDescriptor | 45 | TEST | 2 | EXPECT_FALSE(fd.valid()); / EXPECT_EQ(fd.get(), -1); |
| FileDescriptorTest.DestructorClosesDescriptor | 54 | TEST | 4 | ASSERT_EQ(pipe(pipeFds), 0); / EXPECT_TRUE(fd.valid()); |
| FileDescriptorTest.MoveConstructorTransfersOwnership | 77 | TEST | 8 | ASSERT_EQ(pipe(pipeFds), 0); / ASSERT_TRUE(source.valid()); |
| FileDescriptorTest.MoveAssignmentReleasesOldDescriptor | 110 | TEST | 11 | ASSERT_EQ(pipe(pipeA), 0); / ASSERT_EQ(pipe(pipeB), 0); |
| FileDescriptorTest.SelfMoveAssignmentIsSafe | 151 | TEST | 6 | ASSERT_EQ(pipe(pipeFds), 0); / ASSERT_TRUE(fd.valid()); |

## tests/shared/protocol/EnvelopeCodecTest.cpp

[完整源码](source/tests/shared/protocol/EnvelopeCodecTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| EnvelopeCodecTest.OldClientReadsNewLoginAndPreservesUnknownFields | 29 | TEST | 13 | ASSERT_TRUE(wire); / ASSERT_TRUE(older.ParseFromString(*wire)); |
| EnvelopeCodecTest.NewClientReadsOldPingAndPreservesRetiredTag | 58 | TEST | 11 | ASSERT_TRUE(older.SerializeToString(&wire)); / ASSERT_TRUE(decode(wire, newer)); |
| EnvelopeCodecTest.OtherPayloadsUseStableTags | 80 | TEST | 14 | ASSERT_TRUE(wire); / ASSERT_TRUE(oldLogin.ParseFromString(*wire)); |
| EnvelopeCodecTest.FieldNumbersAreStableAndRetiredTagIsReserved | 110 | TEST | 7 | EXPECT_TRUE(descriptor->IsReservedNumber(2)); / EXPECT_TRUE(descriptor->IsReservedName("legacy_client_tag")); |
| EnvelopeCodecTest.RejectsOversizedPayloadAndLeavesOutputUnchanged | 134 | TEST | 5 | EXPECT_FALSE(EnvelopeCodec::encode(huge)); / ASSERT_TRUE(huge.SerializeToString(&wire)); |
| EnvelopeCodecTest.RejectsDeeplyNestedError | 148 | TEST | 5 | EXPECT_FALSE(EnvelopeCodec::encode(deep)); / ASSERT_TRUE(deep.SerializeToString(&wire)); |
| EnvelopeCodecTest.RejectsMalformedAndPayloadlessMessages | 166 | TEST | 5 | EXPECT_FALSE(decode("", output)); / EXPECT_FALSE(decode(malformed, output)); |

## tests/shared/protocol/PacketCodecTest.cpp

[完整源码](source/tests/shared/protocol/PacketCodecTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| PacketCodecTest.DecodesBytewiseHeadersAndCoalescedBinaryFrames | 32 | TEST | 5 | EXPECT_EQ( / EXPECT_EQ( |
| PacketCodecTest.ProcessesAggregateLargerThanItsBound | 66 | TEST | 4 | EXPECT_EQ( / EXPECT_EQ(payload, "payload"); |
| PacketCodecTest.RejectsConfiguredLengthAsSoonAsPrefixArrives | 92 | TEST | 4 | EXPECT_EQ( / EXPECT_EQ(codec.pendingBytes(), PacketHeader::kPrefixSize); |
| PacketCodecTest.RejectsMalformedHeaderBeforeBodyRetention | 120 | TEST | 3 | EXPECT_EQ( / EXPECT_FALSE(delivered); |
| PacketCodecTest.CallbackStopIsTerminalAndDoesNotDeliverTail | 144 | TEST | 4 | EXPECT_EQ( / EXPECT_EQ(codec.pendingBytes(), 0); |
| PacketCodecTest.ThrowingAndReentrantCallbacksInvalidateDecoder | 165 | TEST | 4 | EXPECT_THROW( / EXPECT_EQ( |
| PacketCodecTest.SupportsSharedFourMiBCeilingAndRejectsInvalidEncode | 198 | TEST | 12 | ASSERT_TRUE(output); / EXPECT_EQ( |
| PacketCodecTest.RejectsBadMagicAtFourBytes | 260 | TEST | 4 | EXPECT_EQ(codec.feed(badMagic, receive), FrameDecodeStatus::Invalid); / EXPECT_EQ(codec.pendingBytes(), badMagic.size()); |

## tests/shared/protocol/PacketHeaderCodecTest.cpp

[完整源码](source/tests/shared/protocol/PacketHeaderCodecTest.cpp.md)

| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |
| --- | ---: | --- | ---: | --- |
| ProtocolBufferTest.PrependsHeaderWithoutMovingPayload | 24 | TEST | 8 | ASSERT_EQ(buffer.prependableBytes(), PacketHeader::kHeaderSize); / ASSERT_TRUE(buffer.append("abc", 3)); |
| ProtocolBufferTest.CompactsBeforeGrowingAndPreservesDataOnFailure | 44 | TEST | 6 | ASSERT_TRUE(buffer.append("abcdefghij", 10)); / ASSERT_TRUE(buffer.retrieve(8)); |
| ProtocolBufferTest.DirectWriteRequiresWritableSpace | 63 | TEST | 5 | ASSERT_TRUE(buffer.hasWritten(4)); / EXPECT_FALSE(buffer.hasWritten(1)); |
| PacketHeaderCodecTest.EncodesExactBigEndianHeaderAndDecodesIt | 77 | TEST | 10 | ASSERT_TRUE(buffer.append("abc", 3)); / ASSERT_TRUE(PacketHeaderCodec::encode(buffer, header)); |
| PacketHeaderCodecTest.RejectsIncompleteAndInvalidLengthsEarly | 114 | TEST | 7 | EXPECT_EQ( / EXPECT_EQ( |
| PacketHeaderCodecTest.RejectsUnknownTypeAndVersion | 149 | TEST | 2 | EXPECT_EQ( / EXPECT_EQ( |
| PacketHeaderCodecTest.EncodeRejectsMismatchedLengthWithoutMutation | 167 | TEST | 5 | ASSERT_TRUE(buffer.append("abc", 3)); / EXPECT_FALSE(PacketHeaderCodec::encode(buffer, header)); |
| PacketHeaderCodecTest.AcceptsMaximumLegalBodyLength | 191 | TEST | 3 | EXPECT_EQ( / EXPECT_EQ(decoded.m_length, header.m_length); |
| PacketHeaderCodecTest.ParsesFragmentedAndCoalescedFrames | 207 | TEST | 8 | EXPECT_TRUE(output.append(payload.data(), payload.size())); / EXPECT_TRUE(PacketHeaderCodec::encode(output, header)); |
| PacketHeaderCodecTest.InvalidLengthStopsBeforePayloadAllocation | 260 | TEST | 4 | ASSERT_TRUE(input.append(oversized, sizeof(oversized))); / EXPECT_EQ( |
| PacketHeaderCodecTest.CallbackCanStopAfterOneFrame | 281 | TEST | 6 | ASSERT_TRUE(PacketHeaderCodec::encode(first, header)); / ASSERT_TRUE(input.append(first.peek(), first.readableBytes())); |
| ProtocolBufferTest.GrowsSafelyAfterPrependingHeader | 306 | TEST | 5 | ASSERT_TRUE(buffer.append("abcde", 5)); / ASSERT_TRUE(buffer.prepend("01234567890", 11)); |
| PacketHeaderCodecTest.OneByteThroughMaximumPayloadBoundaries | 319 | TEST | 14 | ASSERT_TRUE(output.append(payload.data(), payload.size())); / ASSERT_TRUE(PacketHeaderCodec::encode(output, header)); |
| PacketHeaderCodecTest.TenThousandFramesAcrossRandomChunks | 388 | TEST | 12 | ASSERT_TRUE(output.append(payload.data(), payload.size())); / ASSERT_TRUE(PacketHeaderCodec::encode(output, header)); |
| PacketHeaderCodecTest.OversizedAndNegativeLengthBitPatternsAreInvalid | 476 | TEST | 7 | ASSERT_TRUE(input.append(&networkMagic, sizeof(networkMagic))); / ASSERT_TRUE(input.append(&networkLength, sizeof(networkLength))); |
| PacketHeaderCodecTest.EveryTruncatedPrefixOrPayloadNeedsMoreBytes | 520 | TEST | 11 | ASSERT_TRUE(frame.append("abc", 3)); / ASSERT_TRUE(PacketHeaderCodec::encode(frame, header)); |
| PacketHeaderCodecTest.RejectsBadMagicBeforeLengthAndEncode | 574 | TEST | 5 | EXPECT_EQ(PacketHeaderCodec::decode(wire.data(), 3, decoded), / EXPECT_EQ(PacketHeaderCodec::decode(wire.data(), 4, decoded), |
