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
        catch (Exception ex)
        {
            logger.Error(ex, "Error during backup operation: " + ex.Message);
        }
        finally
        {
            semaphore.Release();
        }
    }));
}

await Task.WhenAll(tasks);