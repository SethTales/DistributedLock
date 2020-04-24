using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DistributedLockPOC.Logging
{
    public class Logger
    {
        private const string LogBaseDir = "D:\\DistributedLockPOCLogs";
        private readonly string _logFile;
        public Logger()
        {
            if (!Directory.Exists(LogBaseDir))
            {
                Directory.CreateDirectory(LogBaseDir);
            }

            foreach (var file in Directory.EnumerateFiles(LogBaseDir))
            {
                File.Delete(file);
            }

            _logFile = Path.Combine(LogBaseDir, $"log.txt");
        }

        public void Log(string logPayload)
        {
            lock (_logFile)
            {
                using (var sw = new StreamWriter(_logFile, true))
                {
                    sw.Write($"{logPayload}\n");
                }
            }
        }
    }
}
