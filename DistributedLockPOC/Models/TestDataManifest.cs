using System;
using System.Collections.Generic;
using System.Text;

namespace DistributedLockPOC.Models
{
    public class TestDataManifest
    {
        public string BaseDir { get; set; }
        public List<string> FileNames { get; set; }
    }
}
