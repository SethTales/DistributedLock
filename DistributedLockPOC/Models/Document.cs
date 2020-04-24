using System;
using System.Collections.Generic;
using System.Text;

namespace DistributedLockPOC.Models
{
    public class Document
    {
        public string DocumentId { get; set; }
        public string Title { get; set; }
        public string Author { get; set; }
        public string Contents { get; set; }
        public int Count { get; set; } = 1;
    }
}
