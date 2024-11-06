using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
using Redis_Stack_Tests.Models;
using StackExchange.Redis;
using System.Text.Json;

ConnectionMultiplexer connection = ConnectionMultiplexer.Connect("localhost:6380");

var db = connection.GetDatabase();

//string (classic)

Console.WriteLine("STRING KEY VALUE");
Console.WriteLine();

var stringSetKey = Guid.NewGuid().ToString();

await db.StringSetAsync(stringSetKey, "Hello World");
var stringGetValue = await db.StringGetAsync(stringSetKey);

Console.WriteLine(stringGetValue);

//hash
Console.WriteLine();
Console.WriteLine("HASH");


var hashEntry = new HashEntry[]
{
    new HashEntry("name", "Toyota"),
    new HashEntry("model", "Corolla"),
    new HashEntry("year", "2021")
};

var hashSetKey = "hashset:car:1";

await db.HashSetAsync("hashset:car:1", hashEntry);

var hashGetAll = await db.HashGetAllAsync(hashSetKey);

Console.WriteLine();
foreach (var item in hashGetAll)
{
    Console.WriteLine($"{item.Name}: {item.Value}");
}

Console.WriteLine();
Console.WriteLine("JSON");

var searchCommands = db.FT();
var jsonCommands = db.JSON();

var carOne = new Car(1, 10, "Toyota", DateTime.Now.AddYears(-5).Ticks);
var carTwo = new Car(2, 20, "BMW", DateTime.Now.AddYears(-10).Ticks);
var carThree = new Car(2, 30, "Subaru", DateTime.Now.AddYears(-15).Ticks);

const string CARS_INDEX = "idx:cars";

searchCommands.DropIndex(CARS_INDEX);

var indexSchema = new Schema()
    .AddTextField(new FieldName($"$.{nameof(Car.Make)}", $"{nameof(Car.Make)}"))
    .AddNumericField(new FieldName($"$.{nameof(Car.Vin)}", $"{nameof(Car.Vin)}"))
    .AddNumericField(new FieldName($"$.{nameof(Car.ManufacturedDate)}", $"{nameof(Car.ManufacturedDate)}"));

await searchCommands.CreateAsync(CARS_INDEX, new FTCreateParams().On(IndexDataType.JSON).Prefix("cars:"), indexSchema);

var jsonTestKey = $"cars:1";

var addCarOneTask = jsonCommands.SetAsync(jsonTestKey, "$", carOne);
var addCarTwoTask = jsonCommands.SetAsync($"cars:2", "$", carTwo);
var addCarThreeTask = jsonCommands.SetAsync($"cars:3", "$", carTwo);

await Task.WhenAll(addCarOneTask, addCarTwoTask, addCarThreeTask);

var result = await jsonCommands.GetAsync(jsonTestKey);

var cartest = JsonSerializer.Deserialize<Car>(result.ToString(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

Console.WriteLine("");

var carsByVin = searchCommands.Search(CARS_INDEX, new Query("@Vin:[5 15]")).Documents.Select(x => JsonSerializer.Deserialize<Car>(x["json"])).DistinctBy(x => x.Id).ToList();

var query = new Query($"@ManufacturedDate:[{DateTime.Now.AddYears(-25).Ticks} {DateTime.Now.AddYears(-10).Ticks}]");

var carsByDate = searchCommands.Search(CARS_INDEX, query)
    .Documents.Select(x => JsonSerializer.Deserialize<Car>(x["json"])).DistinctBy(x => x.Id).ToList();

Console.WriteLine("");