using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using DistributedLockPOC;
using DistributedLockPOC.Data;
using DistributedLockPOC.Logging;
using DistributedLockPOC.Models;
using DistributedLockPOC.Utilities;
using Newtonsoft.Json;
using NUnit.Framework;

namespace DistributedLockPOCTests
{
    public class Tests
    {
        private const string BaseDir = "D:\\DistributedLocPOCData";
        private TestDataCreator _dataCreator;
        private List<TestDataManifest> _manifestList;
        private Logger _logger;
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _logger = new Logger();
            _dataCreator = new TestDataCreator();
            var lockDb = new DistributedLockDb(_logger);
            var docDb = new DocumentDb(_logger);
            lockDb.InitializeDatabase();
            docDb.InitializeDatabase();
        }

        [TearDown]
        public void TearDown()
        {
            var lockDb = new TestDistributedLockDb(_logger);
            var docDb = new TestDocumentDb(_logger);
            lockDb.TruncateLocksTable();
            docDb.TruncateDocumentsTable();
        }

        [Test]
        public void Given2ParallelConsumers_ShouldProduce6DocsWithCount2_And1DocWithCount8()
        {
            var threadMap = new ConcurrentDictionary<int, Thread>();
            var lockDb = new TestDistributedLockDb(_logger);
            var docDb = new TestDocumentDb(_logger);
            _manifestList = _dataCreator.CreateTestData(10, 1, 4, BaseDir);
            var documents = new List<Document>();
            foreach (var manifest in _manifestList)
            {
                foreach (var file in manifest.FileNames)
                {
                    using (var sr = new StreamReader(Path.Combine(manifest.BaseDir, file)))
                    {
                        var jsonDocument = sr.ReadToEnd();
                        documents.Add(JsonConvert.DeserializeObject<Document>(jsonDocument));
                    }

                }
            }

            var consumer = new Consumer(lockDb, docDb, _logger);
            foreach (var document in documents)
            {
                var t1 = new Thread(() => consumer.Consume(document, Guid.NewGuid().ToString()));
                var t2 = new Thread(() => consumer.Consume(document, Guid.NewGuid().ToString()));
                threadMap.TryAdd(t1.ManagedThreadId, t1);
                threadMap.TryAdd(t2.ManagedThreadId, t2);
                t1.Start();
                t2.Start();
            }

            while (threadMap.Any(t => t.Value.IsAlive))
            {
                Thread.Sleep(100);
            }

            var savedDocuments = docDb.GetDocumentsByDocumentId(documents.OrderBy(x => x.Title).Select(d => d.DocumentId).Distinct().ToList());
            Assert.AreEqual(7, savedDocuments.Count);
            Assert.AreEqual(8, savedDocuments.Last().Count);
        }
    }
}
