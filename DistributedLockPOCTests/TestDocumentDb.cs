using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DistributedLockPOC.Data;
using DistributedLockPOC.Logging;
using DistributedLockPOC.Models;
using Newtonsoft.Json;
using Npgsql;

namespace DistributedLockPOCTests
{
    internal class TestDocumentDb : DocumentDb
    {
        internal TestDocumentDb(Logger logger) : base(logger)
        {
        }

        internal void TruncateDocumentsTable()
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var truncateCommand =
                    new NpgsqlCommand(
                        "TRUNCATE documents;", conn))
                {
                    truncateCommand.ExecuteNonQuery();
                }
            }
        }

        internal List<Document> GetDocumentsByDocumentId(List<string> documentIds)
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var command =
                    new NpgsqlCommand(
                        $"SELECT document FROM documents WHERE document_id IN ({(documentIds.Count == 1 ? "'" + documentIds.First() + "'" : string.Join(",", documentIds.Select(d => "'" + d + "'")))})",
                        conn))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        var documents = new List<Document>();
                        while (reader.Read())
                        {
                            documents.Add(JsonConvert.DeserializeObject<Document>(reader[0].ToString()));
                        }
                        return documents;
                    }
                }             
            }
        }
    }
}
