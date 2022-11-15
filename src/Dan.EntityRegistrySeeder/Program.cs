// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Text;
using ICSharpCode.SharpZipLib.GZip;
using Mono.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

var options = new Options();
var showHelp = false;
var p = new OptionSet() {
        { "o=|output_path=", "Path where to put all the downloaded files. Will attempt to create if not existing.",
            v => options.OutputDir = v },
        { "k|keep_downloaded", "Keep downloaded file. Default: true",
            v => options.KeepDownloadedFile = v != null },
        { "u|use_downloaded", "Use downloaded file if exists. Default: true",
            v => options.UseDownloadedFile = v != null },
        { "t=|types", "Types to download. Can be either 'enheter','underenheter' or 'both'. Default: 'both'",
            v =>
            {
                options.DownloadUnitTypes = v switch
                {
                    "enheter" => UnitType.Units,
                    "underenheter" => UnitType.SubUnits,
                    "both" => UnitType.Both,
                    _ => throw new OptionException()
                };
            }
        },

        { "h|help",  "show this message and exit",
            v => showHelp = v != null },

    };

try
{
    p.Parse(args);
}
catch (OptionException e)
{
    Console.Write("Dan.EntityRegistrySeeder: ");
    Console.WriteLine(e.Message);
    Console.WriteLine("Try `Dan.EntityRegistrySeeder.exe --help' for more information.");
    return;
}

if (showHelp)
{
    Console.WriteLine("Usage: Dan.EntityRegistrySeeder.exe [OPTIONS]");
    Console.WriteLine("Downloads and prepares a directory for upload to Azure Storage via azcopt.");
    Console.WriteLine();
    Console.WriteLine("Options:");
    p.WriteOptionDescriptions(Console.Out);
}


Console.WriteLine("Initializing download to " + options.OutputDir);
if (!Directory.Exists(options.OutputDir))
{
    Directory.CreateDirectory(options.OutputDir);
}

var taskList = new List<Task>();
switch (options.DownloadUnitTypes)
{
    case UnitType.Units:
        Console.WriteLine("Processing units ...");
        taskList.Add(DownloadFromBrreg(UnitType.Units));
        break;
    case UnitType.SubUnits:
        Console.WriteLine("Processing subunits ...");
        taskList.Add(DownloadFromBrreg(UnitType.SubUnits));
        break;
    case UnitType.Both:
        Console.WriteLine("Processing both types in parallell ...");
        await Task.Run(() =>
        {
            taskList.Add(DownloadFromBrreg(UnitType.SubUnits));
            taskList.Add(DownloadFromBrreg(UnitType.Units));
            
        });
        break;
    default:
        throw new ArgumentOutOfRangeException();
}

await Task.WhenAll(taskList);

Console.WriteLine("All done! Now run azcopy");

async Task DownloadFromBrreg(UnitType unitTypeEnum)
{
    var unitType = unitTypeEnum == UnitType.Units ? "enheter" : "underenheter";

    var outputDir = options.OutputDir + Path.DirectorySeparatorChar + unitType;
    var downloadedFile = options.OutputDir + Path.DirectorySeparatorChar + "downloaded_" + unitType + ".json.gz";
    if (!Directory.Exists(outputDir))
    {
        Directory.CreateDirectory(outputDir);
    }

    Stream inputBuffer;
    if (!options.UseDownloadedFile || !File.Exists(downloadedFile))
    {
        var client = new HttpClient();
        var response = await client.GetAsync("https://data.brreg.no/enhetsregisteret/oppslag/" + unitType + "/lastned",
            HttpCompletionOption.ResponseHeadersRead);

        Console.WriteLine("Download of '" + unitType + "' started ...");

        var contentStream = await response.Content.ReadAsStreamAsync();
        inputBuffer = new MemoryStream();
        await contentStream.CopyToAsync(inputBuffer);
        inputBuffer.Seek(0, SeekOrigin.Begin);

        Console.WriteLine("Download of  '" + unitType + "' complete");

        if (options.KeepDownloadedFile)
        {
            Console.WriteLine("Saving  '" + unitType + "' to '" + downloadedFile + "'");
            var downloadedFileStream = File.Create(downloadedFile);
            await inputBuffer.CopyToAsync(downloadedFileStream);
            inputBuffer.Seek(0, SeekOrigin.Begin);
        }
    }
    else
    {
        Console.WriteLine("Using saved file from '" + downloadedFile + "'");
        inputBuffer = new FileStream(downloadedFile, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize:4096, FileOptions.Asynchronous);
    }
    
    var sw = Stopwatch.StartNew();
    var cnt = 0;

    var tasks = new Queue<Task>();

    await using (var gzipStream = new GZipInputStream(inputBuffer))
    using (var sr = new StreamReader(gzipStream))
    using (var reader = new JsonTextReader(sr))
    {
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.StartObject)
            {
                var entry = JObject.Load(reader).ToObject<JObject>();
                var outputPath = outputDir + Path.DirectorySeparatorChar + entry!["organisasjonsnummer"]!;

                tasks.Enqueue(File.WriteAllTextAsync(outputPath, entry.ToString(Formatting.None), Encoding.UTF8));
            }

            cnt++;

#if DEBUG
            if (cnt % 500 == 0)
            {
                Console.WriteLine(
                    $"Written {cnt} '{unitType}' entries ({(float)cnt / sw.ElapsedMilliseconds * 1000} entries/sec)");
            }
#endif
        }
    }

    await Task.WhenAll(tasks);

    var elapsed = sw.ElapsedMilliseconds;
    var persec = (float)cnt / elapsed * 1000;

    Console.WriteLine($"Wrote {cnt} '{unitType}' entries in {elapsed / 1000} seconds ({persec} entries/sec)");
}


enum UnitType
{
    Both,
    Units,
    SubUnits
}

class Options
{
    public string OutputDir { get; set; } = Environment.CurrentDirectory + Path.DirectorySeparatorChar + "download";
    public UnitType DownloadUnitTypes { get; set;} = UnitType.Both;
    public bool KeepDownloadedFile { get; set; } = true;
    public bool UseDownloadedFile { get; set; } = true;
}


