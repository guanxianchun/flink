/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.DefaultSchedulerComponents.createSchedulerComponents;

/** Factory for {@link DefaultScheduler}. */
public class DefaultSchedulerFactory implements SchedulerNGFactory {

    @Override
    public SchedulerNG createInstance(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final SlotPoolService slotPoolService,
            final ScheduledExecutorService futureExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final Time rpcTimeout,
            final BlobWriter blobWriter,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final Time slotRequestTimeout,
            final ShuffleMaster<?> shuffleMaster,
            final JobMasterPartitionTracker partitionTracker,
            final ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final FatalErrorHandler fatalErrorHandler,
            final JobStatusListener jobStatusListener)
            throws Exception {
        // 1. 将slotPoolService对象类型换成成SlotPool，如果转换失败，则抛出异常
        final SlotPool slotPool =
                slotPoolService
                        .castInto(SlotPool.class)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The DefaultScheduler requires a SlotPool."));
        // 2. 创建Scheduler组件
        final DefaultSchedulerComponents schedulerComponents =
                createSchedulerComponents(
                        jobGraph.getJobType(),
                        jobGraph.isApproximateLocalRecoveryEnabled(),
                        jobMasterConfiguration,
                        slotPool,
                        slotRequestTimeout);
        // 3. 创建重启恢复策略,如：固定延迟重启策略、失败率重启策略、 无重启策略和全图恢复策略
        final RestartBackoffTimeStrategy restartBackoffTimeStrategy =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                                jobGraph.getSerializedExecutionConfig()
                                        .deserializeValue(userCodeLoader)
                                        .getRestartStrategy(),
                                jobMasterConfiguration,
                                jobGraph.isCheckpointingEnabled())
                        .create();
        log.info(
                "Using restart back off time strategy {} for {} ({}).",
                restartBackoffTimeStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());
        // 4. 创建默认的运行拓扑图创建工厂
        final ExecutionGraphFactory executionGraphFactory =
                new DefaultExecutionGraphFactory(
                        jobMasterConfiguration,
                        userCodeLoader,
                        executionDeploymentTracker,
                        futureExecutor,
                        ioExecutor,
                        rpcTimeout,
                        jobManagerJobMetricGroup,
                        blobWriter,
                        shuffleMaster,
                        partitionTracker);
        // 5. 创建默认的调度器
        return new DefaultScheduler(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                schedulerComponents.getStartUpAction(),
                new ScheduledExecutorServiceAdapter(futureExecutor),
                userCodeLoader,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                schedulerComponents.getSchedulingStrategyFactory(),
                FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(jobMasterConfiguration),
                restartBackoffTimeStrategy,
                new DefaultExecutionVertexOperations(),
                new ExecutionVertexVersioner(),
                schedulerComponents.getAllocatorFactory(),
                initializationTimestamp,
                mainThreadExecutor,
                jobStatusListener,
                executionGraphFactory,
                shuffleMaster,
                rpcTimeout);
    }

    @Override
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return JobManagerOptions.SchedulerType.Ng;
    }
}
