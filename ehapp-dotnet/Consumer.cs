

using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Core.Diagnostics;
using Microsoft.Azure.Cosmos;
using System.Diagnostics.Tracing;
using Newtonsoft.Json;
using System.Text;



DotNetEnv.Env.TraversePath().Load();

var consoleListener = AzureEventSourceListener.CreateConsoleLogger(EventLevel.Critical);

var accountName = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME");
var containerName = Environment.GetEnvironmentVariable("STORAGE_CONTAINER_NAME");

var ehNamespace = Environment.GetEnvironmentVariable("EVENT_HUBS_NAMESPACE");
var eventHubName = Environment.GetEnvironmentVariable("EVENT_HUB_NAME");
var consumerGroup = Environment.GetEnvironmentVariable("EVENT_HUB_CONSUMER_GROUP");;
var credential = new DefaultAzureCredential();


var storageClient = new BlobContainerClient(
    new Uri(string.Format("https://{0}.blob.core.windows.net/{1}", accountName, containerName)),
    credential);

var processor = new EventProcessorClient(
    storageClient,
    consumerGroup,
    ehNamespace, eventHubName, credential);

var partitionEventCount = new ConcurrentDictionary<string, int>();

// Cosmos

var cosmosEndpoint = Environment.GetEnvironmentVariable("COSMOS_ENDPOINT");
var databaseId = Environment.GetEnvironmentVariable("COSMOS_DATABASE_ID");
var cmContainerName = Environment.GetEnvironmentVariable("COSMOS_CONTAINER");

// Enable Bulk to optimize for throughput instead of latency
// .NET SDK V3 contains stream APIs that can receive and return data without serializing.

CosmosClient client = new CosmosClient(cosmosEndpoint, credential);
Container container = null;


async Task processEventHandler(ProcessEventArgs args)
{
    try
    {

        // If the cancellation token is signaled, then the
        // processor has been asked to stop.  It will invoke
        // this handler with any events that were in flight;
        // these will not be lost if not processed.
        //
        // It is up to the handler to decide whether to take
        // action to process the event or to cancel immediately.

        if (args.CancellationToken.IsCancellationRequested)
        {
            return;
        }

        string partition = args.Partition.PartitionId;
        byte[] eventBody = args.Data.EventBody.ToArray();

        var jsonString = Encoding.UTF8.GetString(eventBody);
        dynamic document = JsonConvert.DeserializeObject(jsonString);

        // Create a unique Id to be used as the document id
        // set the document id to the partitionid appended with the args.Data.Offset
        document["id"] = $"{partition}-{args.Data.Offset}";

        //Debug.WriteLine($"Event from partition { partition } inserting:  { document.ToString() }.");

        await container.CreateItemAsync(document);//, new PartitionKey("id"));

        
        int eventsSinceLastCheckpoint = partitionEventCount.AddOrUpdate(
            key: partition,
            addValue: 1,
            updateValueFactory: (_, currentCount) => currentCount + 1);

        if (eventsSinceLastCheckpoint >= 50)
        {
            await args.UpdateCheckpointAsync();
            partitionEventCount[partition] = 0;
        }
    }
    catch
    {
        // It is very important that you always guard against
        // exceptions in your handler code; the processor does
        // not have enough understanding of your code to
        // determine the correct action to take.  Any
        // exceptions from your handlers go uncaught by
        // the processor and will NOT be redirected to
        // the error handler.
    }
}

Task processErrorHandler(ProcessErrorEventArgs args)
{
    try
    {
        Debug.WriteLine("Error in the EventProcessorClient");
        Debug.WriteLine($"\tOperation: { args.Operation }");
        Debug.WriteLine($"\tException: { args.Exception }");
        Debug.WriteLine("");
    }
    catch
    {
        // It is very important that you always guard against
        // exceptions in your handler code; the processor does
        // not have enough understanding of your code to
        // determine the correct action to take.  Any
        // exceptions from your handlers go uncaught by
        // the processor and will NOT be handled in any
        // way.
    }

    return Task.CompletedTask;
}

try
{
    /*
    var databaseResponse = await client.CreateDatabaseIfNotExistsAsync(databaseId);
    var database = databaseResponse.Database;

    ContainerProperties containerProperties = new ContainerProperties(databaseId, partitionKeyPath: "/id");
    container = await database.CreateContainerIfNotExistsAsync(
                containerProperties,
                throughput: 1000);
    */
    container = client.GetContainer(databaseId, cmContainerName);

    using var cancellationSource = new CancellationTokenSource();
    //cancellationSource.CancelAfter(TimeSpan.FromSeconds(30));

    processor.ProcessEventAsync += processEventHandler;
    processor.ProcessErrorAsync += processErrorHandler;

    try
    {
        await processor.StartProcessingAsync(cancellationSource.Token);

        // Enable the EventSource loggers to view Information events about partition load balancing.
        var source = EventSource.GetSources().FirstOrDefault(s => s.Name == "Azure-Messaging-EventHubs-Processor-PartitionLoadBalancer");
        if (source != null)
        {
            consoleListener.EnableEvents(source, EventLevel.Informational);
        }

        await Task.Delay(Timeout.Infinite, cancellationSource.Token);
    }
    catch (TaskCanceledException)
    {
        // This is expected if the cancellation token is
        // signaled.
    }
    finally
    {
        // This may take up to the length of time defined
        // as part of the configured TryTimeout of the processor;
        // by default, this is 60 seconds.

        await processor.StopProcessingAsync();
    }
}
catch
{
    // The processor will automatically attempt to recover from any
    // failures, either transient or fatal, and continue processing.
    // Errors in the processor's operation will be surfaced through
    // its error handler.
    //
    // If this block is invoked, then something external to the
    // processor was the source of the exception.
}
finally
{
   // It is encouraged that you unregister your handlers when you have
   // finished using the Event Processor to ensure proper cleanup.  This
   // is especially important when using lambda expressions or handlers
   // in any form that may contain closure scopes or hold other references.

   processor.ProcessEventAsync -= processEventHandler;
   processor.ProcessErrorAsync -= processErrorHandler;
}