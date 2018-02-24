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
  public static async Task<(long Next, long Prev, byte [] Data)> ReadRecord(this Stream stream, CancellationToken tok = new CancellationToken())
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

    return (Next: next, Prev: prev, Data: record);
  }

  public static async Task<(long Next, long Prev, long Size)> WriteRecord(this Stream stream, byte [] data, CancellationToken tok = new CancellationToken())
  {
    var prev = stream.Position;
    var next = prev + data.Length + (sizeof(long)*2);
    await stream.WriteAsync(BitConverter.GetBytes(next), 0, sizeof(long), tok);
    await stream.WriteAsync(data, 0, data.Length, tok);
    await stream.WriteAsync(BitConverter.GetBytes(prev), 0, sizeof(long), tok);
    await stream.FlushAsync(tok);
    return (next, prev, data.Length);
  }
}

public static class Program
{
  public static async Task<(double Time, T Result, Exception Exception)> TimeAsync<T>(Func<Task<T>> value) 
  {
    var stopWatch = new Stopwatch();
    stopWatch.Start();
    try {
      var t = await value();
      stopWatch.Stop();
      return (stopWatch.Elapsed.TotalSeconds, t, null);
    }
    catch(Exception ex){
      stopWatch.Stop();
      return (stopWatch.Elapsed.TotalSeconds, default(T), ex);
    }
  }

  public static async Task Main(string[] args)
  {
    // NoAlloc:  Ellapsed 5.8809285, Writes: 1000000, Size: 70000008, Ex:
    //           Ellapsed 5.9026677, Writes: 1000000, Size: 70000008, Ex:
    //           Ellapsed 5.7378225, Writes: 1000000, Size: 70000008, Ex:
    // PreAlloc: Ellapsed 5.5682736, Writes: 1000000, Size: 70000008, Ex:
    WriteLine("flat-app");
    using (var stream = File.Open("data", FileMode.OpenOrCreate))
    {
      var result = await TimeAsync(async () =>
      {
        var i = 0;
        for (; i < 10; i++)
        {
          var h = await stream.WriteRecord(BitConverter.GetBytes((long)i));
          
          WriteLine($"Next: {h.Next}, Prev: {h.Prev}, Size: {h.Size}");
        }
        return i;
      });

      WriteLine($"Ellapsed {result.Time}, Writes: {result.Result}, Size: {stream.Length}, Ex: {result.Exception}");
    }
    ReadLine();
  }
}
