using System.IO.Compression;
using Bytewizer.Backblaze.Client;
using Bytewizer.Backblaze.Models;

//0 Configure logging and the Services/DI
NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
Services services = new Services(logger);

//1 Build the appsettings.json IConfiguration into our custom AppSettings class
AppSettings appSettings = await services.LoadAppSettingsAsync();

//2 Connect to BackBlaze. This is safe to reuse across concurrent async upload. Meant to be reused. you can and should reuse IStorageClient (your BackblazeClient) for multiple file uploads. Unlike SMB2Client, it's designed to be reused safely.
IStorageClient backblazeClient = await services.ConnectToBackblazeAsync();

//3 Get the Buckets, then Join the bucket and its corresponding backup plan. Then do LINQ to Check if the bucket's ID exists in any of the backup plans.
var bucketsWithPlans = await services.GetBucketsWithPlansAsync(backblazeClient);

//4 This is everything we have now to do a backup operation. Copying a file from the NAS to BackBlaze. Long term we can make this loop parallel.
var semaphore = new SemaphoreSlim(appSettings.threadCount);
var tasks = new List<Task>();
foreach (var item in bucketsWithPlans)
{
    await semaphore.WaitAsync();

    tasks.Add(Task.Run(async () =>
    {
        try
        {         
            BucketItem bucket = item.bucket;
            BackupPlan plan = item.plan;

            if (plan.type == BackupPlanType.VM && plan.enabled) //VM
            {
                await services.VMBackupAsync(bucket, plan, backblazeClient);
            }
            else if (plan.type == BackupPlanType.SQL && plan.enabled) //SQL
            {
                await services.SQLBackupAsync(bucket, plan, backblazeClient);
            }
            else if (plan.type == BackupPlanType.QuickBooks && plan.enabled) //QuickBooks
            {
                await services.QuickBooksBackupAsync(bucket, plan, backblazeClient);
            }
        }
        finally
        {
            semaphore.Release();
        }
    }));

    // Console.WriteLine("Uploading test file...");
    // using (var stream = File.OpenRead(@"/home/stefonalfaro/Desktop/SelfConversionToJudaism.pdf"))
    // {
    //     var jsonSettings = new JsonSerializerSettings
    //     {
    //         NullValueHandling = NullValueHandling.Ignore,
    //         Error = (sender, args) =>
    //         {
    //             args.ErrorContext.Handled = true; // Continue serialization, skip problematic property
    //         }
    //     };

    //     var results = await Client.UploadAsync(bucket.BucketId, "/SelfConversionToJudaism22.pdf", stream);
    //     Console.WriteLine(JsonConvert.SerializeObject(results.Response, jsonSettings));

    //     var results2 = await Client.Parts.StartLargeFileAsync(bucket.BucketId, "/SelfConversionToJudaism22.pdf");
    //     var fileId = results2.Response.FileId;
    //     //var results3 = await Client.Parts.UploadAsync("", 1, null, stream, null);
    // }
}
await Task.WhenAll(tasks);

// Connect to NAS via SMB
// Console.WriteLine("Connecting to NAS: " + appSettings.localNASConfig.description);
// var client = new SMB2Client();
// bool isConnected = client.Connect(IPAddress.Parse(appSettings.localNASConfig.ip), SMBTransportType.DirectTCPTransport);
// if (isConnected)
// {
//     NTStatus status =  client.Login(String.Empty, appSettings.localNASConfig.username, appSettings.localNASConfig.password);
//     Console.WriteLine("SMB Login Status: " + status.ToString());
//     if (status == NTStatus.STATUS_SUCCESS)
//     {
//         var fileStore = client.TreeConnect(appSettings.localNASConfig.shareName, out status);
//         if (status == NTStatus.STATUS_SUCCESS)
//         {
//             // Create a handle to the directory
//             object directoryHandle;
//             FileStatus fileStatus;
//             status = fileStore.CreateFile(out directoryHandle, out fileStatus, "xo-vm-backups\\26b1c840-eeb0-3b0a-7551-efb8177d573f", AccessMask.GENERIC_READ, SMBLibrary.FileAttributes.Directory, ShareAccess.Read, CreateDisposition.FILE_OPEN, CreateOptions.FILE_DIRECTORY_FILE, null);
//             if (status == NTStatus.STATUS_SUCCESS)
//             {
//                 // Now query the directory
//                 List<QueryDirectoryFileInformation> files;
//                 status = fileStore.QueryDirectory(out files, directoryHandle, "*", FileInformationClass.FileDirectoryInformation);
//                 Console.WriteLine($"Files count: {files?.Count ?? 0}");

//                 if (status == NTStatus.STATUS_SUCCESS || status == NTStatus.STATUS_NO_MORE_FILES)
//                 {
//                     foreach (var file in files)
//                     {
//                         if (file is FileDirectoryInformation fileInfo)  // Cast to FileDirectoryInformation to access properties
//                         {
//                             if (fileInfo.FileName != "." && fileInfo.FileName != "..") // Skip . and .. entries
//                             {
//                                 string fileType = fileInfo.FileAttributes.HasFlag(SMBLibrary.FileAttributes.Directory) ? "DIR" : "FILE";
//                                 Console.WriteLine($"{fileInfo.FileName} - {fileType} - Size: {fileInfo.EndOfFile} - Uploaded: {fileInfo.LastWriteTime}");
//                             }
//                         }
//                     }
//                 }
//                 else
//                 {
//                     logger.Error("Failed to query directory: " + status.ToString());
//                 }

//                 // Close the directory handle
//                 fileStore.CloseFile(directoryHandle);
//             }
//             else
//             {
//                 logger.Error("Failed to open directory: " + status.ToString());
//             }

//             fileStore.Disconnect();
//         }
//         else
//         {
//             logger.Error("SMB Tree Connect failed: " + status.ToString());
//         }
//     }
//     else
//     {
//         logger.Error("SMB Login failed: " + status.ToString());
//     }

//     client.Disconnect();
// }