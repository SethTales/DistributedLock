using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DistributedLockPOC.Logging;
using DistributedLockPOC.Models;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Npgsql;

namespace DistributedLockPOC.Data
{
    public class DistributedLockDb : DbContext
    {
        protected const string ConnectionString = "Host=localhost;Port=5432;Username=postgres";

        private readonly Logger _logger;

        public DistributedLockDb(Logger logger)
        {
            _logger = logger;
        }

        public void InitializeDatabase()
        {
            GenerateRandomLatency();
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var createLocksTableCommand = new NpgsqlCommand(
                    "CREATE TABLE IF NOT EXISTS locks (document_id varchar(32) NOT NULL, UNIQUE(document_id));", conn))
                {
                    createLocksTableCommand.ExecuteNonQuery();
                }
            }
        }

        public bool Lock(string documentId, string correlationId)
        {
            GenerateRandomLatency();
            try
            {
                using (var conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (var insertLockCommand =
                        new NpgsqlCommand(
                            @"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; " +
                            $"INSERT INTO locks (document_id) VALUES('{documentId}'); " +
                            @"COMMIT;", conn))
                    {
                        var lockResult = insertLockCommand.ExecuteNonQuery();
                        _logger.Log($"CorrelationId: {correlationId} - acquired lock for document {documentId} at {DateTime.UtcNow:O}");
                        return lockResult == 1;
                    }
                }
            }
            catch (PostgresException npgsqlException)
            {
                if (npgsqlException.Data.Contains("MessageText") && npgsqlException.Data["MessageText"].ToString()
                        .Contains("duplicate key value violates unique constraint"))
                {
                    _logger.Log($"CorrelationId: {correlationId} - HANDLED EXCEPTION THROWN: {JsonConvert.SerializeObject(npgsqlException)}");
                    return false;
                }

                throw;
            }
        }

        public void Unlock(string documentId, string correlationId)
        {
            GenerateRandomLatency();
            try
            {
                using (var conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (var insertLockCommand =
                        new NpgsqlCommand(
                            @"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; " +
                            $"DELETE FROM locks WHERE document_id = '{documentId}'; " +
                            @"COMMIT;", conn))
                    {
                        insertLockCommand.ExecuteNonQuery();
                        _logger.Log($"CorrelationId: {correlationId} - released lock for document {documentId} at {DateTime.UtcNow:O}");
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Log($"CorrelationId: {correlationId} - EXCEPTION THROWN: {JsonConvert.SerializeObject(e)}");
                throw;
            }
        }

        public bool IsLocked(string documentId, string correlationId)
        {
            GenerateRandomLatency();
            try
            {
                using (var conn = new NpgsqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (var insertLockCommand =
                        new NpgsqlCommand(
                            @"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; " +
                            $"SELECT document_id FROM locks WHERE document_id = '{documentId}'; " +
                            @"COMMIT;", conn))
                    {
                        using (var reader = insertLockCommand.ExecuteReader())
                        {
                            var isLocked = reader.HasRows;
                            _logger.Log($"CorrelationId: {correlationId} - document {documentId} isLocked result = {isLocked} at {DateTime.UtcNow:O}");
                            return isLocked;
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

        private void GenerateRandomLatency()
        {
            var random = new Random();
            Thread.Sleep(random.Next(0, 100));
        }
    }
}
