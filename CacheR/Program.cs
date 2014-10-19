using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using Newtonsoft.Json;
using System.Threading;
using ProtoBuf;
using System.IO;

namespace Cachely
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // http://kkovacs.eu/cassandra-vs-mongodb-vs-couchdb-vs-redis
            // Figure out based on the abatement conditions what data we need, store the last n values that match this requirement
            // That way we only ever store as much as we need, memory use is kept under control
            // Database load is also kept under control as the only load occurs when reading the conditions
            //var redisServer = System.Diagnostics.Process.Start("..\\..\\..\\packages\\Redis-64.2.8.17\\redis-server.exe");
            //Thread.Sleep(5000);
            //var redisCli = System.Diagnostics.Process.Start("..\\..\\..\\packages\\Redis-64.2.8.17\\redis-cli.exe");
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            //var data = DoStuffProtobuf(redis);
            var data = DoStuffJson(redis);
            data.Wait();
            //Console.WriteLine(JsonConvert.DeserializeObject<ProcessData>();
            //Console.WriteLine(pdsCache.Sum(x => x.Value) / pdsCache.Count());
            Console.WriteLine(data.Result.Count());
            // Wait for input
            Console.ReadKey();
        }

        private static async Task<RedisValue[]> DoStuffProtobuf(ConnectionMultiplexer redis)
        {
            IDatabase db = redis.GetDatabase();

            var pds = new List<ProcessData>();

            var random = new Random();

            for (int i = 0; i < 1000000; i++)
            {
                pds.Add(new ProcessData { Id = Guid.NewGuid(), EndDate = DateTime.Now, Name = "bar", Value = random.Next(0, 1000) });
            }

            //var stream = new MemoryStream();
            //ProtoBuf.Serializer.Serialize<List<ProcessData>>(stream, pds);

            var redisValues = pds.Select(x =>
            {
                var s = new MemoryStream();
                Serializer.Serialize(s, x);
                s.Close();
                return (RedisValue)s.ToString();
            }).ToArray();

            await db.KeyDeleteAsync("data");
            //redisValues.ForEach(x => db.ListLeftPushAsync("data", x));
            //db.ListLeftPushAsync("data", stream.ToString());
            await db.ListLeftPushAsync("data", redisValues);

            var data = await db.ListRangeAsync("data", 0, 1);

            //var pdsCache = data.Select(x =>
            //{
            //    var s = new MemoryStream(x);
            //    var pdc = Serializer.Deserialize<ProcessData>(s);
            //    s.Close();
            //    return pdc;
            //});
            return data;
        }

        private static async Task<RedisValue[]> DoStuffJson(ConnectionMultiplexer redis)
        {
            IDatabase db = redis.GetDatabase();

            var pds = new List<ProcessData>();

            var random = new Random();

            for (int i = 0; i < 1000000; i++)
            {
                pds.Add(new ProcessData { Id = Guid.NewGuid(), EndDate = DateTime.Now, Name = "bar", Value = random.Next(0, 1000) });
            }

            //var stream = new MemoryStream();
            //ProtoBuf.Serializer.Serialize<List<ProcessData>>(stream, pds);

            var redisValues = pds.Select(x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray();

            await db.KeyDeleteAsync("data");
            //redisValues.ForEach(x => db.ListLeftPushAsync("data", x));
            //db.ListLeftPushAsync("data", stream.ToString());
            await db.ListLeftPushAsync("data", redisValues);

            var data = await db.ListRangeAsync("data", 0, -2);

            return data;
        }
    }

    [ProtoContract]
    public class ProcessData
    {
        [ProtoMember(1)]
        public Guid Id { get; set; }
        [ProtoMember(2)]
        public string Name { get; set; }
        [ProtoMember(3)]
        public DateTime EndDate { get; set; }
        [ProtoMember(4)]
        public double Value { get; set; }
    }
}
