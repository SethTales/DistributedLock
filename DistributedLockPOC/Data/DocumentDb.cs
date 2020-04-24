using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using DistributedLockPOC.Logging;
using DistributedLockPOC.Models;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Npgsql;

namespace DistributedLockPOC.Data
{
    public class DocumentDb : DbContext
    {
        protected const string ConnectionString = "Host=localhost;Port=5432;Username=postgres";

        private readonly Logger _logger;

        public DocumentDb(Logger logger)
        {
            _logger = logger;
        }

        public void InitializeDatabase()
        {
            GenerateRandomLatency();
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var createDocumentsTableCommand = new NpgsqlCommand(
                    "CREATE TABLE IF NOT EXISTS documents (document_id varchar(32) NOT NULL, document json NOT NULL, UNIQUE(document_id));",
                    conn))
                {
                    createDocumentsTableCommand.ExecuteNonQuery();
                }
            }
        }

        public (bool, Document) GetDocumentIfExists(string documentId, string correlationId)
        {
            GenerateRandomLatency();
            try
            {
                using (var conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (var getDocumentCommand =
                        new NpgsqlCommand(
                            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; " +
                            $"SELECT document FROM documents WHERE document_id = '{documentId}'; " +
                            "COMMIT;", conn))
                    {
                        using (var reader = getDocumentCommand.ExecuteReader())
                        {
                            reader.Read();
                            var getDocIfExistsResult = reader.HasRows ? (true, JsonConvert.DeserializeObject<Document>(reader[0].ToString())) : (false, null);
                            _logger.Log($"CorrelationId: {correlationId} - getDocIfExistsResult = {getDocIfExistsResult.Item1} for document {getDocIfExistsResult.Item2?.DocumentId ?? "NOT FOUND"} at {DateTime.UtcNow:O}");
                            return getDocIfExistsResult;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Log($"CorrelationId: {correlationId} - EXCEPTION THROWN: {JsonConvert.SerializeObject(e)}");
                throw;
            }
        }

        public void InsertDocument(Document document, string correlationId)
        {
            GenerateRandomLatency();
            var jsonDocument = JsonConvert.SerializeObject(document);
            try
            {
                using (var conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (var insertDocumentCommand =
                        new NpgsqlCommand(
                            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; " +
                            $"INSERT INTO documents VALUES ('{document.DocumentId}', '{jsonDocument}'); " +
                            "COMMIT;", conn))
                    {
                        insertDocumentCommand.ExecuteNonQuery();
                        _logger.Log(
                            $"CorrelationId: {correlationId} - inserted new document {document.DocumentId} at {DateTime.UtcNow:O}");
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Log($"CorrelationId: {correlationId} - EXCEPTION THROWN: {JsonConvert.SerializeObject(e)}");
                throw;
            }
        }

        public void UpdateDocument(Document document, string correlationId)
        {
            GenerateRandomLatency();
            var jsonDocument = JsonConvert.SerializeObject(document);
            try
            {
                using (var conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (var updateDocumentCommand =
                        new NpgsqlCommand(
                            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; " +
                            $"UPDATE documents SET document = '{jsonDocument}' WHERE document_id = '{document.DocumentId}';" +
                            "COMMIT;",
                            conn))
                    {
                        updateDocumentCommand.ExecuteNonQuery();
                        _logger.Log($"CorrelationId: {correlationId} - update document {document.DocumentId} at {DateTime.UtcNow:O}");
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Log($"CorrelationId: {correlationId} - EXCEPTION THROWN: {JsonConvert.SerializeObject(e)}");
                throw;
            }
        }

        private void GenerateRandomLatency()
        {
            var random = new Random();
            Thread.Sleep(random.Next(0, 100));
        }
    }
}
