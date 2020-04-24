using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using DistributedLockPOC.Models;
using Newtonsoft.Json;

namespace DistributedLockPOC.Utilities
{
    public class TestDataCreator
    {
        public List<TestDataManifest> CreateTestData(int totalDocumentCount, int numberOfDocumentSets, int duplicateDocumentCount,
            string directoryLocation)
        {
            var testDataManifestList = new List<TestDataManifest>();
            if (!Directory.Exists(directoryLocation))
            {
                Directory.CreateDirectory(directoryLocation);
            }
            var uniqueDocumentCount = totalDocumentCount - duplicateDocumentCount;
            for (var i = 0; i < numberOfDocumentSets; i++)
            {
                var baseDir = Path.Combine(directoryLocation, $"TestDocSet-{i + 1}");
                if (!Directory.Exists(baseDir))
                {
                    Directory.CreateDirectory(baseDir);
                }
                else
                {
                    foreach (var file in Directory.EnumerateFiles(baseDir))
                    {
                        File.Delete(Path.Combine(baseDir, file));
                    }
                }

                var testDataManifest = new TestDataManifest
                {
                    BaseDir = baseDir,
                    FileNames = new List<string>()
                };
                testDataManifestList.Add(testDataManifest);

                for (var n = 0; n < uniqueDocumentCount; n++)
                {
                    var contents = RandomString(50);
                    var document = new Document
                    {
                        DocumentId = CreateMD5(contents),
                        Title = $"Document-{n + 1}",
                        Author = $"Author-{n + 1}",
                        Contents = contents
                    };
                    var jsonDocument = JsonConvert.SerializeObject(document);
                    var fileName = $"Document-{n + 1}";

                    using (var sr = new StreamWriter(Path.Combine(baseDir, fileName)))
                    {
                        sr.Write(jsonDocument);
                    }

                    testDataManifest.FileNames.Add(fileName);
                }

                var dupeContents = RandomString(50);
                var docId = CreateMD5(dupeContents);
                for (var k = uniqueDocumentCount; k < totalDocumentCount; k++)
                {
                    var document = new Document
                    {
                        DocumentId = docId,
                        Title = $"Document-{k + 1}",
                        Author = $"Author-{k + 1}",
                        Contents = dupeContents
                    };
                    var jsonDocument = JsonConvert.SerializeObject(document);
                    var fileName = $"Document-{k + 1}";
                    using (var sr = new StreamWriter(Path.Combine(baseDir, fileName)))
                    {
                        sr.Write(jsonDocument);
                    }

                    testDataManifest.FileNames.Add(fileName);
                }
            }

            return testDataManifestList;
        }

        private static string RandomString(int length)
        {
            var random = new Random();

            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                return new string(Enumerable.Repeat(chars, length)
                    .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public static string CreateMD5(string input)
        {
            // Use input string to calculate MD5 hash
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                foreach (var b in hashBytes)
                {
                    sb.Append(b.ToString("X2"));
                }
                return sb.ToString();
            }
        }
    }
}
