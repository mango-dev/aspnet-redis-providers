//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using StackExchange.Redis;

namespace Microsoft.Web.Redis
{
    internal class RedisSharedConnection
    {
        private ProviderConfiguration _configuration;
        private ConfigurationOptions _configOption;
        private Lazy<ConnectionMultiplexer> _redisMultiplexer;

        internal static DateTimeOffset lastReconnectTime = DateTimeOffset.MinValue;
        internal static DateTimeOffset firstErrorTime = DateTimeOffset.MinValue;
        internal static DateTimeOffset previousErrorTime = DateTimeOffset.MinValue;
        static object reconnectLock = new object();
        internal static TimeSpan ReconnectFrequency = TimeSpan.FromSeconds(60);
        internal static TimeSpan ReconnectErrorThreshold = TimeSpan.FromSeconds(30);

        /// <summary>
        /// 是否启用CC配置
        /// </summary>
        private bool ccBindState =>ConfigCenterHelper.ccBindState;
        /// <summary>
        /// CC中redis的集群Host信息
        /// </summary>
        private string ccRedisHostsStr => ConfigCenterHelper.ccRedisHostsStr;

        // Used for mocking in testing
        internal RedisSharedConnection()
        { }

        public RedisSharedConnection(ProviderConfiguration configuration)
        {
            _configuration = configuration;

            // If connection string is given then use it otherwise use individual options
            if (!string.IsNullOrEmpty(configuration.ConnectionString))
            {
                _configOption = ConfigurationOptions.Parse(configuration.ConnectionString);
                // Setting explicitly 'abortconnect' to false. It will overwrite customer provided value for 'abortconnect'
                // As it doesn't make sense to allow to customer to set it to true as we don't give them access to ConnectionMultiplexer
                // in case of failure customer can not create ConnectionMultiplexer so right choice is to automatically create it by providing AbortOnConnectFail = false
                _configOption.AbortOnConnectFail = false;
            }
            else
            {
                _configOption = new ConfigurationOptions();
                if (configuration.Port == 0)
                {
                    _configOption.EndPoints.Add(configuration.Host);
                }
                else
                {
                    _configOption.EndPoints.Add(configuration.Host + ":" + configuration.Port);
                }
                _configOption.Password = configuration.AccessKey;
                _configOption.Ssl = configuration.UseSsl;
                _configOption.AbortOnConnectFail = false;

                if (configuration.ConnectionTimeoutInMilliSec != 0)
                {
                    _configOption.ConnectTimeout = configuration.ConnectionTimeoutInMilliSec;
                }

                if (configuration.OperationTimeoutInMilliSec != 0)
                {
                    _configOption.SyncTimeout = configuration.OperationTimeoutInMilliSec;
                }
            }
            ConfigCenterHelper.ccBindState = configuration.CCBindState;
            if (ccBindState)
            {
                //CC中获取Redis的连接信息
                //赋初值
                ConfigCenterHelper.ccRedisHostsStr = configuration.Host + ":" + configuration.Port;
                //拉取绑定CC的ccRedisHostsStr
                BuildCC();
            }
            else
            {
                CreateMultiplexer();
            }
        }

        public IDatabase Connection
        {
            get { return _redisMultiplexer.Value.GetDatabase(_configOption.DefaultDatabase ?? _configuration.DatabaseId); }
        }

        public void ForceReconnect()
        {
            var previousReconnect = lastReconnectTime;
            var elapsedSinceLastReconnect = DateTimeOffset.UtcNow - previousReconnect;

            lock (reconnectLock)
            {
                var utcNow = DateTimeOffset.UtcNow;
                elapsedSinceLastReconnect = utcNow - lastReconnectTime;

                var elapsedSinceFirstError = utcNow - firstErrorTime;
                var elapsedSinceMostRecentError = utcNow - previousErrorTime;
                previousErrorTime = utcNow;

                LogUtility.LogInfo($"ForceReconnect: now: {utcNow.ToString()}");
                LogUtility.LogInfo($"ForceReconnect: elapsedSinceLastReconnect: {elapsedSinceLastReconnect.ToString()}, ReconnectFrequency: {ReconnectFrequency.ToString()}");
                LogUtility.LogInfo($"ForceReconnect: elapsedSinceFirstError: {elapsedSinceFirstError.ToString()}, elapsedSinceMostRecentError: {elapsedSinceMostRecentError.ToString()}, ReconnectErrorThreshold: {ReconnectErrorThreshold.ToString()}");

                firstErrorTime = DateTimeOffset.MinValue;
                previousErrorTime = DateTimeOffset.MinValue;

                var oldMultiplexer = _redisMultiplexer;
                CloseMultiplexer(oldMultiplexer);
                CreateMultiplexer();
            }
        }

        private void CreateMultiplexer()
        {
            if (ccBindState)
            {
                CreateMultiplexerByCC();
            }
            else
            {
                if (LogUtility.logger == null)
                {
                    _redisMultiplexer = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(_configOption));
                }
                else
                {
                    _redisMultiplexer = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(_configOption, LogUtility.logger));
                }
                lastReconnectTime = DateTimeOffset.UtcNow;
            }
        }

        private void CloseMultiplexer(Lazy<ConnectionMultiplexer> oldMultiplexer)
        {
            if (oldMultiplexer.Value != null)
            {
                try
                {
                    oldMultiplexer.Value.Close();
                }
                catch (Exception)
                {
                    // Example error condition: if accessing old.Value causes a connection attempt and that fails. 
                }
            }
        }

        #region CC中获取Redis连接信息并创建复用连接

        public void UpdateMultiplexer(string CCRedisHostsStr)
        {
            if (!string.IsNullOrEmpty(CCRedisHostsStr))
            {
                LogUtility.LogWarning("UpdateMultiplexer => {0}", CCRedisHostsStr);
                ConfigCenterHelper.ccRedisHostsStr = CCRedisHostsStr;
                ForceReconnect();
            }
        }

        /// <summary>
        /// 构建一个订阅CC变化自动更新的连接实例
        /// </summary>
        /// <returns></returns>
        private void BuildCC()
        {
            ConfigCenterHelper.GetCodisMangoProxyAddrConfig((key, values) =>
            {
                UpdateMultiplexer(values);
            });

            //创建连接
            CreateMultiplexerByCC();
        }

        /// <summary>
        /// 构建一个从CC获取的Redis集群的连接实例
        /// </summary>
        /// <returns></returns>
        private void CreateMultiplexerByCC()
        {

            string redisMasterHostsStr = ccRedisHostsStr.TrimEnd(',') + ",";

            var constr = string.Format("{0}DefaultDatabase={1}", redisMasterHostsStr, _configOption.DefaultDatabase == null ? 0 : _configOption.DefaultDatabase);
            LogUtility.LogWarning("redisMasterHostsStr => {0}", constr);

            if (LogUtility.logger == null)
            {
                _redisMultiplexer = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(constr));
            }
            else
            {
                _redisMultiplexer = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(constr, LogUtility.logger));
            }
            lastReconnectTime = DateTimeOffset.UtcNow;
        }

        #endregion CC中获取Redis连接信息并创建复用连接

    }
}
