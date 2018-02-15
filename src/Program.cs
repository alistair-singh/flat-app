using System;
using System.IO;
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
  public long Previous { get; set; } = 1;
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
      var next = Stream.Position + buffer.Length;
      writer.Seek(0, SeekOrigin.Begin);
      writer.Write(next);
      Previous = next;

      RecordSize = Math.Max(RecordSize, (int)buffer.Length);

      Stream.Seek(0, SeekOrigin.End);
      await Stream.WriteAsync(buffer.GetBuffer(), 0, (int)buffer.Length);
      return Previous;
    }
  }

  public static async Task<Journal> Empty()
  {
    var journal = new Journal();
    await journal.Stream.WriteAsync(BitConverter.GetBytes(0L), 0, sizeof(long));
    return journal;
  }
}

public static class Program
{
  public static async Task Main(string[] args)
  {
  }
}
