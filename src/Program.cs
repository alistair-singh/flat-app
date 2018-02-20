using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using static System.Console;

public interface IJournal
{
  Task<T> Read<T>(long id);
  Task<long> Write<T>(T t);
}

public class Journal : IJournal
{
  private int RecordSize { get; set; } = 256;
  public long Previous { get; set; } = 8;
  public Stream Stream { get; set; } = new MemoryStream();
  public BinaryFormatter Formatter { get; } = new BinaryFormatter();

  public Task<T> Read<T>(long id)
  {
    lock (this)
    {
      Stream.Seek(id, SeekOrigin.Begin);
      return Task.FromResult((T)Formatter.Deserialize(Stream));
    }
  }

  public async Task<long> Write<T>(T t)
  {
    using (var buffer = new MemoryStream(RecordSize + sizeof(long) * 2))
    using (var writer = new BinaryWriter(buffer))
    {
      writer.Seek(sizeof(long), SeekOrigin.Current);
      Formatter.Serialize(buffer, t);
      writer.Write(Previous);
      var next = Stream.Position + buffer.Length + sizeof(long);
      writer.Seek(0, SeekOrigin.Begin);
      writer.Write(next);
      Previous = next;

      RecordSize = Math.Max(RecordSize, (int)buffer.Length);

      Stream.Seek(0, SeekOrigin.End);
      await Stream.WriteAsync(buffer.GetBuffer(), 0, (int)buffer.Length);
      return next;
    }
  }

  public static async Task<Journal> Empty(Stream stream = null)
  {
    var journal = new Journal();
    if(stream != null) {
      journal.Stream = stream;
      journal.Stream.SetLength(journal.Previous);
    }
    await journal.Stream.WriteAsync(BitConverter.GetBytes(journal.Previous), 0, sizeof(long));
    return journal;
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
    //var journal = await Journal.Empty();
    //var journal = await Journal.Empty(new MemoryStream(new byte[80000008]));
    var journal = await Journal.Empty(File.Open("data", FileMode.OpenOrCreate));

    var result = await TimeAsync(async ()=>
    {
      var i = 0;
      for(; i < 10; i++) 
      {
        await journal.Write(i);
      }
      return i;
    });

    WriteLine($"Ellapsed {result.Time}, Writes: {result.Result}, Size: {journal.Stream.Length}, Ex: {result.Exception}");
    ReadKey(true);
  }
}
