using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;

namespace RedisSessionStateProviderWebNet450_cc
{
    public class Logger
    {
        public static TextWriter GetTextWriter()
        {
            return new StreamWriter(@"D:\log\SessionProvider450CC.txt");
        }
    }
}