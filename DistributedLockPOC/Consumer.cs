using System.Threading;
using System.Threading.Tasks;
using DistributedLockPOC.Data;
using DistributedLockPOC.Logging;
using DistributedLockPOC.Models;

namespace DistributedLockPOC
{
    public class Consumer
    {
        private readonly DistributedLockDb _lockDb;
        private readonly DocumentDb _documentDb;
        private readonly Logger _logger;

        public Consumer(DistributedLockDb lockDb, DocumentDb documentDb, Logger logger)
        {
            _lockDb = lockDb;
            _documentDb = documentDb;
            _logger = logger;
        }

        public void Consume(Document document, string correlationId)
        {
            var threadSleep = 200;
            while (_lockDb.IsLocked(document.DocumentId, correlationId))
            {
                Thread.Sleep(threadSleep);
            }

            while (!_lockDb.Lock(document.DocumentId, correlationId))
            {
                _logger.Log($"CorrelationId: {correlationId} - Unable to acquire lock for document {document.DocumentId}, sleeping for {threadSleep} MS");
                Thread.Sleep(threadSleep);
            }

            var (documentExists, existingDocument) = _documentDb.GetDocumentIfExists(document.DocumentId, correlationId);
            if (documentExists)
            {
                existingDocument.Count++;
                _documentDb.UpdateDocument(existingDocument, correlationId);
            }
            else
            {
                _documentDb.InsertDocument(document, correlationId);
            }

            _lockDb.Unlock(document.DocumentId, correlationId);
        }
    }
}
