/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.AbstractContainerizedClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link ClusterClientFactory} for a YARN cluster. */
@Internal
public class YarnClusterClientFactory
        extends AbstractContainerizedClusterClientFactory<ApplicationId> {
    /**
     * 检查-t/--target参数值是否为：yarn-per-job , yarn-session , yarn-application
     * @param configuration
     * @return
     */
    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        checkNotNull(configuration);
        final String deploymentTarget = configuration.getString(DeploymentOptions.TARGET);
        return YarnDeploymentTarget.isValidYarnTarget(deploymentTarget);
    }

    /**
     * 创建YarnClient描述对象
     * @param configuration containing the configuration options relevant for the {@link
     *     org.apache.flink.client.deployment.ClusterDescriptor}
     * @return
     */
    @Override
    public YarnClusterDescriptor createClusterDescriptor(Configuration configuration) {
        checkNotNull(configuration);
        //1. 获取部署配置临时目录
        final String configurationDirectory = configuration.get(DeploymentOptionsInternal.CONF_DIR);
        //2. 设置yarn日志文件目录
        YarnLogConfigUtil.setLogConfigFileInConfig(configuration, configurationDirectory);
        //3. 获取要部署的Yan集群描述对象
        return getClusterDescriptor(configuration);
    }

    @Nullable
    @Override
    public ApplicationId getClusterId(Configuration configuration) {
        checkNotNull(configuration);
        final String clusterId = configuration.getString(YarnConfigOptions.APPLICATION_ID);
        return clusterId != null ? ConverterUtils.toApplicationId(clusterId) : null;
    }

    @Override
    public Optional<String> getApplicationTargetName() {
        return Optional.of(YarnDeploymentTarget.APPLICATION.getName());
    }

    private YarnClusterDescriptor getClusterDescriptor(Configuration configuration) {
        //1. 创建Yarn客户端
        final YarnClient yarnClient = YarnClient.createYarnClient();
        //2. 创建Yarn配置对象
        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
        //3. 客户端初始化yarn配置
        yarnClient.init(yarnConfiguration);
        //4. 启动yarn客户服务(historyService)
        yarnClient.start();
        //5. 返回Yarn集群的描述
        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);
    }
}
