using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using CCUtil;
using Microsoft.Office.Interop.Excel; // Add COM reference: Microsoft Excel 16.0 Object Library
using System.Globalization;
using System.IO.Compression;
using System.Threading;

namespace EPICCFastExcel
{
    public class ExcelToSqlLoader
    {
        // Delegates
        public event statusDelegate evtStatus;
        public event errorDelegate evtError;

        public delegate void statusDelegate(string strLogMessage, string strShortUIMessage);
        public delegate void errorDelegate(Exception X);

        // Config
        private const int JsonInsertBatchRows = 50000;   // tune to your service limits/timeouts
        private const bool UseCompression = true;        // GZIP payload for density
        private const int MaxFixedLength = 4000;         // NVARCHAR(N) up to 4000; else NVARCHAR(MAX)
        private const string DefaultDbUrl = "https://epicc.seatec.cloud/epiccservices/";

        public void LoadExcelIntoNewTable(string dbKey, string tableName, string excelPath, string startingBatch, string dbUrl = DefaultDbUrl)
        {
            try
            {
                // Validate
                if (string.IsNullOrWhiteSpace(dbKey)) throw new ArgumentException("db_key is required", nameof(dbKey));
                if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("table_name is required", nameof(tableName));
                if (string.IsNullOrWhiteSpace(excelPath)) throw new ArgumentException("excel_path is required", nameof(excelPath));
                if (!File.Exists(excelPath)) throw new FileNotFoundException($"Excel file not found: {excelPath}", excelPath);

                // Ensure STA thread for COM
                Exception workerEx = null;
                var done = new ManualResetEvent(false);

                Thread t = new Thread(() =>
                {
                    try
                    {
                        evtStatus?.Invoke($"Starting Excel COM load: {excelPath}", "Reading Excel...");
                        var (headers, rows, colMaxLens) = ReadExcel_Interop(excelPath);
                        evtStatus?.Invoke($"Excel read complete: {rows.Count} rows, {headers.Count} columns", $"Read complete ({rows.Count} rows)");

                        string createSql = BuildCreateTableSql(tableName, headers, colMaxLens);
                        evtStatus?.Invoke("Prepared CREATE TABLE statement", "Preparing table...");

                        var db = new CCWebServices();
                        evtStatus?.Invoke($"Connecting to DB: {dbUrl}", "Connecting...");
                        if (!db.Connect("", dbKey, dbUrl))
                            throw new Exception($"DB connect failed: {db.errorMessage}");
                        evtStatus?.Invoke("Connected to DB", "Connected");

                        try
                        {
                            if (!db.Execute(createSql))
                                throw new Exception($"Table create failed: {db.errorMessage}");

                            // Replace the fixed batching loop in LoadExcelIntoNewTable with this adaptive version:
                            int total = rows.Count;
                            int sent = 0;

                            // Adaptive controls
                            int minBatch = 5000;
                            int maxBatch = 150000;
                            bool fixedBatch = startingBatch.Substring(0, 1) == "=";
                            int batch = startingBatch.Substring(0,1) == "=" ? Int32.Parse(startingBatch.Substring(1)) : Int32.Parse(startingBatch);  // startBatch
                            double targetSeconds = 3.0;
                            double fastThreshold = 0.75 * targetSeconds;  // 2.25s
                            double slowThreshold = 1.50 * targetSeconds;  // 4.5s
                            double incFactor = 1.20;
                            double decFactor = 0.80;
                            double ewmaAlpha = 0.25;  // smoothing for rows/sec
                            double ewmaRowsPerSec = 0.0;

                            while (sent < total)
                            {
                                int remaining = total - sent;
                                int take = Math.Min(batch, remaining);
                                var batchRows = rows.Skip(sent).Take(take).ToList();

                                // Build SQL
                                string sqlBatch = BuildInsertBatchSql(tableName, headers, colMaxLens, batchRows, UseCompression);

                                // Execute with timing
                                var sw = System.Diagnostics.Stopwatch.StartNew();
                                bool ok = db.Execute(sqlBatch);
                                sw.Stop();

                                double seconds = Math.Max(sw.Elapsed.TotalSeconds, 0.001);
                                double rps = batchRows.Count / seconds;
                                // EWMA update
                                ewmaRowsPerSec = (ewmaRowsPerSec == 0.0) ? rps : (ewmaAlpha * rps + (1.0 - ewmaAlpha) * ewmaRowsPerSec);

                                if (!ok)
                                {
                                    // Error-aware backoff
                                    string err = (string) db.errorMessage ?? "unknown";
                                    evtStatus?.Invoke(
                                        $"Batch failed (rows={batchRows.Count}, {seconds:F2}s). Error: {err}",
                                        "Batch failed, reducing size...");
                                    batch = Math.Max((int)(batch * decFactor), minBatch);

                                    // Optional: detect specific error hints to be more conservative
                                    if (err.IndexOf("timeout", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                        err.IndexOf("request size", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                        err.IndexOf("payload too large", StringComparison.OrdinalIgnoreCase) >= 0)
                                    {
                                        batch = Math.Max((int)(batch * decFactor), minBatch);
                                    }

                                    // Do not advance 'sent'; retry this segment with smaller batch
                                    continue;
                                }

                                // Success: commit progress
                                sent += batchRows.Count;
                                evtStatus?.Invoke(
                                    $"Inserted {sent}/{total}. Batch={batchRows.Count}, {seconds:F2}s ({rps:F0} rows/s, EWMA={ewmaRowsPerSec:F0}).",
                                    $"Progress {sent}/{total}");

                                // Adaptive tuning on success
                                if (!fixedBatch && seconds < fastThreshold)
                                {
                                    // Fast → increase
                                    batch = Math.Min((int)(batch * incFactor), maxBatch);
                                    evtStatus?.Invoke($"Increasing batch to {batch} (fast: {seconds:F2}s).", $"Progress {sent}/{total}");
                                }
                                else if (!fixedBatch && seconds > slowThreshold)
                                {
                                    // Slow → decrease
                                    batch = Math.Max((int)(batch * decFactor), minBatch);
                                    evtStatus?.Invoke($"Decreasing batch to {batch} (slow: {seconds:F2}s).", $"Progress {sent}/{total}");
                                }
                                else if (fixedBatch) 
                                {
                                    // Within target band: hold
                                    // Optionally use small additive increase:
                                    int additive = Math.Max(batch / 20, 1000); // +5% or 1000 min
                                    batch = Math.Min(batch + additive, maxBatch);
                                    evtStatus?.Invoke($"Maintaining batch near {batch} ({seconds:F2}s).", $"Progress {sent}/{total}");
                                }
                            }
                        }
                        finally
                        {
                            try { db.disconnect(); evtStatus?.Invoke("DB disconnected", "Disconnected"); }
                            catch (Exception ex) { evtError?.Invoke(new Exception($"Disconnect error: {ex.Message}", ex)); }
                        }
                    }
                    catch (Exception ex)
                    {
                        workerEx = ex;
                    }
                    finally
                    {
                        done.Set();
                    }
                });

                t.SetApartmentState(ApartmentState.STA);
                t.IsBackground = true;
                t.Start();

                // Wait for completion
                done.WaitOne();

                if (workerEx != null)
                {
                    evtError?.Invoke(workerEx);
                    throw workerEx;
                }
            }
            catch (Exception ex)
            {
                evtError?.Invoke(ex);
                throw;
            }
        }

        // Bulk read via COM: Application -> Workbook -> Worksheet -> UsedRange -> Value2 (object[,])
        private static (List<string> headers, List<Dictionary<string, string>> rows, Dictionary<string, int> colMaxLens)
            ReadExcel_Interop(string excelPath)
        {
            Application xlApp = null;
            Workbooks xlBooks = null;
            Workbook xlBook = null;
            Sheets xlSheets = null;
            Worksheet xlSheet = null;
            Range usedRange = null;

            try
            {
                xlApp = new Application();
                xlApp.DisplayAlerts = false;
                xlApp.Visible = false;

                xlBooks = xlApp.Workbooks;
                xlBook = xlBooks.Open(Filename: excelPath, ReadOnly: true);
                xlSheets = xlBook.Worksheets;
                xlSheet = (Worksheet)xlSheets.Item[1];

                usedRange = xlSheet.UsedRange;
                int rowCount = usedRange.Rows.Count;
                int colCount = usedRange.Columns.Count;

                // Read all values at once
                object[,] data = (object[,])usedRange.Value2;

                // Headers: row=1
                var headers = new List<string>(colCount);
                for (int c = 1; c <= colCount; c++)
                {
                    string h = NormalizeHeader(ToText(data[1, c]));
                    headers.Add(h);
                }

                var rows = new List<Dictionary<string, string>>(rowCount - 1);
                var maxLens = headers.ToDictionary(h => h, h => 0);

                // Rows: start at row=2
                for (int r = 2; r <= rowCount; r++)
                {
                    var dict = new Dictionary<string, string>();
                    for (int c = 1; c <= colCount; c++)
                    {
                        string header = headers[c - 1];
                        string val = ToText(data[r, c]);
                        dict[header] = val;

                        int len = val.Length;
                        if (len > maxLens[header]) maxLens[header] = len;
                    }
                    rows.Add(dict);
                }

                // Ensure minimum length 1
                for (int i = 0; i < headers.Count; i++)
                {
                    string h = headers[i];
                    if (maxLens[h] < 1) maxLens[h] = 1;
                }

                return (headers, rows, maxLens);
            }
            finally
            {
                // Cleanup COM
                SafeRelease(usedRange);
                SafeRelease(xlSheet);
                SafeRelease(xlSheets);

                if (xlBook != null)
                {
                    try { xlBook.Close(false); } catch { }
                    SafeRelease(xlBook);
                }
                SafeRelease(xlBooks);

                if (xlApp != null)
                {
                    try { xlApp.Quit(); } catch { }
                    SafeRelease(xlApp);
                }

                // Final GC to clear COM RCWs
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        private static void SafeRelease(object com)
        {
            if (com != null)
            {
                try { Marshal.ReleaseComObject(com); }
                catch { }
            }
        }

        private static string ToText(object value)
        {
            if (value == null) return string.Empty;

            // Excel Value2:
            // - Text/strings: string
            // - Numbers: double
            // - Dates: double (OADate). Without per-cell NumberFormat, we can either raw or as date
            // Keep simple: convert doubles invariantly; optionally attempt OADate -> ISO if plausible
            if (value is double d)
            {
                // Heuristic: treat values in Excel date range as date
                // Excel dates roughly >= 1 and <= ~ 2958465 (year 9999)
                if (d >= 1 && d <= 2958465)
                {
                    try
                    {
                        DateTime dt = DateTime.FromOADate(d);
                        return dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    }
                    catch
                    {
                        // fallback to numeric
                    }
                }
                return d.ToString(CultureInfo.InvariantCulture);
            }

            return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        }

        // CREATE TABLE: NVARCHAR(N) if N<=4000, else NVARCHAR(MAX)
        private static string BuildCreateTableSql(string tableName, List<string> headers, Dictionary<string, int> maxLens)
        {
            var cols = new List<string>();
            foreach (var h in headers)
            {
                int n = maxLens[h];
                string type = (n <= MaxFixedLength) ? $"NVARCHAR({n})" : "NVARCHAR(MAX)";
                cols.Add($"{EscapeIdent(h)} {type} NULL");
            }
            string colList = string.Join(",\r\n    ", cols);

            var sb = new StringBuilder();
            sb.AppendLine($"IF OBJECT_ID(N'dbo.{EscapeIdent(tableName)}') IS NULL");
            sb.AppendLine("BEGIN");
            sb.AppendLine($"    CREATE TABLE dbo.{EscapeIdent(tableName)} (");
            sb.AppendLine($"    {colList}");
            sb.AppendLine("    );");
            sb.AppendLine("END;");
            return sb.ToString();
        }

        // Build INSERT batch using JSON + optional GZIP base64 + OPENJSON
        private static string BuildInsertBatchSql(
            string tableName,
            List<string> headers,
            Dictionary<string, int> maxLens,
            List<Dictionary<string, string>> batchRows,
            bool compress)
        {
            string json = BuildJsonArray(batchRows, headers);
            var sb = new StringBuilder();

            if (compress)
            {
                byte[] jsonBytes = Encoding.Unicode.GetBytes(json); // UTF-16 for NVARCHAR
                byte[] gzipped = CompressGzip(jsonBytes);
                string base64 = Convert.ToBase64String(gzipped);

                sb.AppendLine("DECLARE @payload_base64 nvarchar(max) = N'" + EscapeSqlString(base64) + "';");
                sb.AppendLine("DECLARE @bin varbinary(max) = CAST(N'' as xml).value('xs:base64Binary(sql:variable(\"@payload_base64\"))', 'varbinary(max)');");
                sb.AppendLine("DECLARE @json nvarchar(max) = CAST(DECOMPRESS(@bin) AS nvarchar(max));");
            }
            else
            {
                sb.AppendLine("DECLARE @json nvarchar(max) = N'" + EscapeSqlString(json) + "';");
            }

            var withCols = new List<string>();
            foreach (var h in headers)
            {
                int n = maxLens[h];
                string type = (n <= MaxFixedLength) ? $"NVARCHAR({n})" : "NVARCHAR(MAX)";
                withCols.Add($"{EscapeIdent(h)} {type} '$.{EscapeJsonPath(h)}'");
            }
            string withClause = string.Join(",\r\n        ", withCols);
            string colIdList = string.Join(", ", headers.Select(EscapeIdent));

            sb.AppendLine($"INSERT INTO dbo.{EscapeIdent(tableName)} WITH (TABLOCK) ({colIdList})");
            sb.AppendLine("SELECT " + colIdList);
            sb.AppendLine("FROM OPENJSON(@json)");
            sb.AppendLine("WITH (");
            sb.AppendLine("        " + withClause);
            sb.AppendLine(") AS j;");

            return sb.ToString();
        }

        private static string BuildJsonArray(List<Dictionary<string, string>> rows, List<string> headers)
        {
            var sb = new StringBuilder(64 * rows.Count);
            sb.Append('[');
            for (int i = 0; i < rows.Count; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append('{');
                for (int c = 0; c < headers.Count; c++)
                {
                    if (c > 0) sb.Append(',');
                    string h = headers[c];
                    string val = rows[i].TryGetValue(h, out var v) ? v ?? "" : "";
                    sb.Append('"').Append(EscapeJsonString(h)).Append('"').Append(':')
                      .Append('"').Append(EscapeJsonString(val)).Append('"');
                }
                sb.Append('}');
            }
            sb.Append(']');
            return sb.ToString();
        }

        // Utilities
        private static string EscapeIdent(string ident) => "[" + ident.Replace("]", "]]") + "]";
        private static string EscapeSqlString(string s) => (s ?? string.Empty).Replace("'", "''");
        private static string EscapeJsonPath(string h) => h;

        private static string EscapeJsonString(string s)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            var sb = new StringBuilder(s.Length + 16);
            foreach (var ch in s)
            {
                switch (ch)
                {
                    case '\\': sb.Append("\\\\"); break;
                    case '\"': sb.Append("\\\""); break;
                    case '\b': sb.Append("\\b"); break;
                    case '\f': sb.Append("\\f"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (char.IsControl(ch))
                            sb.Append("\\u" + ((int)ch).ToString("x4"));
                        else
                            sb.Append(ch);
                        break;
                }
            }
            return sb.ToString();
        }

        private static byte[] CompressGzip(byte[] data)
        {
            using (var ms = new MemoryStream())
            {
                using (var gz = new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: true))
                {
                    gz.Write(data, 0, data.Length);
                }
                return ms.ToArray();
            }
        }

        private static string NormalizeHeader(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) raw = "Column";
            string s = raw.Trim();
            s = s.Replace("\r", " ").Replace("\n", " ").Replace("\t", " ");
            s = new string(s.Select(ch => char.IsLetterOrDigit(ch) ? ch : '_').ToArray());
            if (string.IsNullOrWhiteSpace(s)) s = "Column";
            return s;
        }
    }
}