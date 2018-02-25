using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using static System.Console;
public static class JournalExtensions
{
  public static async Task<(long Id, long Next, byte [] Data)> NextRecord(this Stream stream, CancellationToken tok = new CancellationToken())
  {
    var startPosition = stream.Position;

    var headerFooterBuffer = new byte[sizeof(long)];
    var read = await stream.ReadAsync(headerFooterBuffer, 0, headerFooterBuffer.Length, tok);
    if(read != headerFooterBuffer.Length) throw new Exception($"Corrupt header [length = {read}]");
    var next = BitConverter.ToInt64(headerFooterBuffer, 0);

    var length = (next - sizeof(long)*2) - startPosition;
    var record = new byte[length];
    read = await stream.ReadAsync(record, 0, record.Length, tok);
    if(read != record.Length) throw new Exception($"Corrupt data [length = {read}]");

    read = await stream.ReadAsync(headerFooterBuffer, 0, headerFooterBuffer.Length, tok);
    var prev = BitConverter.ToInt64(headerFooterBuffer, 0);
    if(read != headerFooterBuffer.Length) throw new Exception($"Corrupt footer [length = {read}]");
    if(prev != startPosition) throw new Exception($"Corrupt record [next = {next}, prev = {prev}, record size = {record.Length}]");

    return (Id: prev, Next: next, Data: record);
  }

  public static async Task<(long Id, long Next, byte [] Data)> PrevRecord(this Stream stream, CancellationToken tok = new CancellationToken())
  {
    var startPosition = stream.Position;
    var headerFooterBuffer = new byte[sizeof(long)];
    stream.Seek(-headerFooterBuffer.Length, SeekOrigin.Current);
    var read = await stream.ReadAsync(headerFooterBuffer, 0, headerFooterBuffer.Length);
    var prev = BitConverter.ToInt64(headerFooterBuffer, 0);
    if(read != headerFooterBuffer.Length) throw new Exception($"Corrupt footer [length = {read}]");
    var record = await stream.ReadRecord(prev, tok);
    stream.Seek(prev - startPosition, SeekOrigin.Current);
    return record;
  }

  public static async Task<(long Id, long Next, byte[] Data)> ReadRecord(this Stream stream, long id, CancellationToken tok = new CancellationToken())
  {
    var startPosition = stream.Position;
    if (startPosition != id) stream.Seek(id - startPosition, SeekOrigin.Current);
    return await stream.NextRecord(tok);
  }

  public static async Task<(long Id, long Prev, long Next, byte[] Data)> ReadHeader(this Stream stream, long id, CancellationToken tok = new CancellationToken())
  {
    var length = stream.Length;
    if (id > length || id < 0) throw new Exception($"Id out of range. {id}");

    if (id == 0)
    {
      var header = new byte[sizeof(long)];
      stream.Seek(id, SeekOrigin.Begin);
      var read = await stream.ReadAsync(header, 0, header.Length);
      if (read != sizeof(long)) throw new Exception($"Corrupt header [length = {read}]");
      var next = BitConverter.ToInt64(header, 0);

      var len = (next - sizeof(long) * 2) - id;
      var record = new byte[len];
      read = await stream.ReadAsync(record, 0, record.Length, tok);
      if(read != record.Length) throw new Exception($"Corrupt data [length = {read}]");

      read = await stream.ReadAsync(header, 0, header.Length, tok);
      var prev = BitConverter.ToInt64(header, 0);
      if (prev != id) throw new Exception($"Corrupt record [next = {next}, prev = {prev}, record size = {record.Length}]");
      return (prev, long.MinValue, next, record);
    }
    
    if (id == length)
    {
      var header = new byte[sizeof(long)];
      stream.Seek(id - sizeof(long), SeekOrigin.Begin);
      var read = await stream.ReadAsync(header, 0, sizeof(long));
      if (read != sizeof(long)) throw new Exception($"Corrupt header [length = {read}]");
      var prev = BitConverter.ToInt64(header, 0);
      return (id, prev, long.MaxValue, null);
    }

    {
      if (id - sizeof(long) != stream.Position) stream.Seek(id - sizeof(long), SeekOrigin.Begin);
      var header = new byte[sizeof(long) * 2];
      var read = await stream.ReadAsync(header, 0, header.Length);
      if (read != header.Length) throw new Exception($"Corrupt header [length = {read}]");
      var prev = BitConverter.ToInt64(header, 0);
      var next = BitConverter.ToInt64(header, sizeof(long));

      var len = (next - sizeof(long) * 2) - id;
      var record = new byte[len];
      read = await stream.ReadAsync(record, 0, record.Length, tok);
      if(read != record.Length) throw new Exception($"Corrupt data [length = {read}]");

      read = await stream.ReadAsync(header, 0, header.Length, tok);
      var prev2 = BitConverter.ToInt64(header, 0);
      if (prev2 != id) throw new Exception($"Corrupt record [next = {next}, prev = {prev}, record size = {record.Length}]");
      return (prev2, prev, next, record);
    }
  }

  public static async Task<(long Id, long Next)> WriteRecord(this Stream stream, byte[] data, CancellationToken tok = new CancellationToken())
  {
    var prev = stream.Position;
    var next = prev + data.Length + (sizeof(long) * 2);
    await stream.WriteAsync(BitConverter.GetBytes(next), 0, sizeof(long), tok);
    await stream.WriteAsync(data, 0, data.Length, tok);
    await stream.WriteAsync(BitConverter.GetBytes(prev), 0, sizeof(long), tok);
    await stream.FlushAsync(tok);
    return (Id: prev, Next: next);
  }
}

public static class Program
{
  public static async Task<(double Time, T Result, Exception Exception)> TimeAsync<T>(Func<Task<T>> value)
  {
    var stopWatch = new Stopwatch();
    stopWatch.Start();
    try
    {
      var t = await value();
      stopWatch.Stop();
      return (stopWatch.Elapsed.TotalSeconds, t, null);
    }
    catch (Exception ex)
    {
      stopWatch.Stop();
      return (stopWatch.Elapsed.TotalSeconds, default(T), ex);
    }
  }

  public static async Task Main(string[] args)
  {
    WriteLine("flat-app");
    using (var raw = File.Open("data", FileMode.OpenOrCreate))
    using (var stream = new BufferedStream(raw, 1024))
    // using (var stream = new MemoryStream())
    {
      var result = await TimeAsync(async () =>
      {
        const long n = 1000L;
        var writes = 0L;
        for (; writes < n; writes++)
        {
          var h = await stream.WriteRecord(BitConverter.GetBytes((long)n - writes));

          // WriteLine($"WRITE Id: {h.Id}, Next: {h.Next}");
        }

        var id = 0L;
        var reads = 0L;
        while(id < stream.Length)
        {
          var h = await stream.ReadRecord(id);
          //WriteLine($"READ Id: {h.Id}, Next: {h.Next}, Value: {BitConverter.ToInt64(h.Data, 0)}");
          id = h.Next;
          reads++;
        }

        id = stream.Length;
        stream.Seek(0, SeekOrigin.End);
        while(id > 0) {
          var h = await stream.PrevRecord();
          //WriteLine($"READ REV Id: {h.Id}, Next: {h.Next}, Value: {BitConverter.ToInt64(h.Data, 0)}");
          id = h.Id;
          reads++;
        }

        id = 0;
        while (id < stream.Length)
        {
          var h = await stream.ReadHeader(id);

          // WriteLine($"READ HEADER Id: {h.Id} Prev: {h.Prev}, Next: {h.Next}, Value: {BitConverter.ToUInt64(h.Data ?? new byte[8], 0)}");
          id = h.Next;
          reads++;
        }

        id = stream.Length;
        while (id >= 0)
        {
          var h = await stream.ReadHeader(id);

          //WriteLine($"READ HEADER REV Id: {h.Id} Prev: {h.Prev}, Next: {h.Next}, Value: {BitConverter.ToUInt64(h.Data ?? new byte[8], 0)}");
          id = h.Prev;
          reads++;
        }
        return (Writes: writes, Reads: reads);
      });

      WriteLine($"Ellapsed {result.Time}, Writes: {result.Result.Writes}, Reads: {result.Result.Reads}, Size: {stream.Length}, Ex: {result.Exception}");
    }
    ReadLine();
  }
}
