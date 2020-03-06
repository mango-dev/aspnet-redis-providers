using MangoMis.Frame.FrameConfigs.Center;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Web.Redis
{
    public static class ConfigCenterHelper
    {
        /// <summary>
        /// 是否启用CC配置
        /// </summary>
        public static bool ccBindState = false;
        /// <summary>
        /// CC中redis的集群Host信息
        /// key:Codis_codis-mango_proxy_addr
        /// val:192.168.4.78:6378,192.168.4.79:6378
        /// </summary>
        public static string ccRedisHostsStr = "";

        private static AfterCenterConfigReload after;

        public static void GetCodisMangoProxyAddrConfig(AfterCenterConfigReload changeCall)
        {
            CenterConfigItem CodisMangoProxyAddrConfig = CenterConfig.GetItem("Codis_codis-mango_proxy_addr",
            CodisMangoProxyAddrConfigChange);
            if (!string.IsNullOrEmpty(CodisMangoProxyAddrConfig.Value))
            {
                ccRedisHostsStr = CodisMangoProxyAddrConfig.Value;
            }
            after = changeCall;
            isInit = true;

        }

        /// <summary>
        /// 判断是否为第一次，防止第一次CC触发回调
        /// </summary>
        private static bool isInit = false;

        /// <summary>
        /// CC中redis的集群Host信息变更后更新Host
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public static void CodisMangoProxyAddrConfigChange(string key, string value)
        {
            if (isInit && !string.IsNullOrWhiteSpace(value))
            {
                ccRedisHostsStr = value;
                LogUtility.LogWarning("CodisMangoProxyAddrConfigChange => {0}", ccRedisHostsStr);
                after(key, value);
            }
        }
    }
}
