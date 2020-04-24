using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DistributedLockPOC.Data;
using DistributedLockPOC.Logging;
using DistributedLockPOC.Models;
using Npgsql;

namespace DistributedLockPOCTests
{
    internal class TestDistributedLockDb : DistributedLockDb
    {
        internal TestDistributedLockDb(Logger logger) : base(logger)
        {
        }
        internal void TruncateLocksTable()
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var truncateCommand =
                    new NpgsqlCommand(
                        "TRUNCATE locks;", conn))
                {
                    truncateCommand.ExecuteNonQuery();
                }
            }
        }
    }
}
