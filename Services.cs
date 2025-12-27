using System.Net;
using Bytewizer.Backblaze.Client;
using Bytewizer.Backblaze.Models;
using Microsoft.Extensions.Configuration;
using SMBLibrary;
using SMBLibrary.Client;

public class Services
{
    NLog.Logger logger;
    AppSettings appSettings;
    ParallelOptions parallelOptions;

    public Services(NLog.Logger logger)
    {
        this.logger = logger;
    }

    public async Task<AppSettings> LoadAppSettingsAsync()
    {
        logger.Warn("Starting GAPP Express Backup Service.");

        //1 Create IConfiguration and bind to AppSettings class
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
        var appSettings = new AppSettings();
        configuration.GetSection("AppSettings").Bind(appSettings);

        logger.Debug($"Thread Count is set to {appSettings.threadCount} and fileLoopThreadCount {appSettings.fileLoopThreadCount}");

        //2 Validate required settings
        if (String.IsNullOrWhiteSpace(appSettings.localNASConfig.username))
        {
            throw new Exception("Local NAS username is not set in appsettings.json");
        }
        if (String.IsNullOrWhiteSpace(appSettings.localNASConfig.password))
        {
            throw new Exception("Local NAS password is not set in appsettings.json");
        }
        if (String.IsNullOrWhiteSpace(appSettings.localNASConfig.ip))
        {
            throw new Exception("Local NAS IP is not set in appsettings.json");
        }
        if (String.IsNullOrWhiteSpace(appSettings.localNASConfig.shareName))
        {
            throw new Exception("Local NAS shareName is not set in appsettings.json");
        }
        if (String.IsNullOrWhiteSpace(appSettings.backblazeConfig.keyId))
        {
            throw new Exception("BackBlaze keyId is not set in appsettings.json");
        }
        if (String.IsNullOrWhiteSpace(appSettings.backblazeConfig.applicationKey))
        {
            throw new Exception("BackBlaze applicationKey is not set in appsettings.json");
        }
        if (appSettings.backupPlans == null || appSettings.backupPlans.Length == 0)
        {
            throw new Exception("No backup plans defined in appsettings.json");
        }

        if (appSettings.testMode)
            logger.Warn("Running in TEST MODE - No changes will be made to BackBlaze or NAS");

        logger.Info($"Chunk Size is set to {appSettings.chunkSizeInMB} MB");

        //3 Set the class level appSettings
        this.appSettings = appSettings;

        parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = appSettings.fileLoopThreadCount
        };

        //4 Return the appSettings in case the calling thread needs it
        return appSettings;
    }

    public async Task<IStorageClient> ConnectToBackblazeAsync()
    {
        IStorageClient client = new BackblazeClient();
        client.Connect(appSettings.backblazeConfig.keyId, appSettings.backblazeConfig.applicationKey);
        return client;
    }

    public async Task<IEnumerable<BucketWithPlan>> GetBucketsWithPlansAsync(IStorageClient client)
    {
        var buckets = await client.Buckets.GetAsync();
        logger.Info($"Found {buckets.ToList().Count} buckets");

        // Join the bucket and its corresponding backup plan
        var bucketsWithPlans = from bucket in buckets
                               join plan in appSettings.backupPlans on bucket.BucketId equals plan.bucketId
                               select new BucketWithPlan { bucket = bucket, plan = plan };

        return bucketsWithPlans;
    }

    public async Task<bool> validateMaxFileSizeToUploadInGB(long bytes)
    {
        //1 Convert bytes to GB
        double gb = bytes / (1024.0 * 1024.0 * 1024.0);
        if (gb > appSettings.maxFileSizeToUploadInGB)
        {
            logger.Debug($"File size {gb} GB exceeds maxFileSizeToUploadInGB setting of {appSettings.maxFileSizeToUploadInGB} GB");
            return false;
        }

        //2 If not exceeded return true
        return true;
    }

    public async Task<bool> validateChunkSizeInMB(long bytes)
    {
        //1 Convert bytes to MB
        double mb = bytes / (1024.0 * 1024.0);
        logger.Debug($"File size is {mb} MB");
        if (mb > appSettings.chunkSizeInMB)
        {
            logger.Debug($"File size {mb} MB exceeds chunkSizeInMB setting of {appSettings.chunkSizeInMB} MB. Will do a large file upload.");
            return true;
        }

        //2 If not exceeded return true
        return false;
    }

    //This only shows the parent file
    public async Task<List<FileItem>> GetFilesFromBackBlazeAsync(IStorageClient client,string bucketId)
    {
        var allFiles = new List<FileItem>(capacity: 12000);
        string? startFileName = null;

        while (true)
        {
            var request = new ListFileNamesRequest(bucketId)
            {
                StartFileName = startFileName,     
                MaxFileCount = 1000, // B2 recommends 1000/page; >1000 may be billed as multiple txns.
                // Prefix = "", // optionally set if you upload under a folder prefix
            };

            var response = await client.Files.ListNamesAsync(request);
            var page = response.Response;

            if (page?.Files is { Count: > 0 })
            {
                allFiles.AddRange(page.Files);
            }

            if (string.IsNullOrEmpty(page?.NextFileName))
            {
                break;
            }

            startFileName = page.NextFileName;
        }

        return allFiles;
    }

    //This shows the duplicated files with same name but different upload date
    public async Task<List<FileItem>> GetFileVersionsFromBackBlazeAsync(IStorageClient client,string bucketId)
    {
        var allFiles = new List<FileItem>(capacity: 12000);
        string? startFileName = null;
        string? startFileId = null;

        while (true)
        {
            var request = new ListFileVersionRequest(bucketId)
            {
                StartFileName = startFileName,
                StartFileId = startFileId,
                MaxFileCount = 1000,
            };

            var response = await client.Files.ListVersionsAsync(request);
            var page = response.Response;

            if (page?.Files is { Count: > 0 })
            {
                allFiles.AddRange(page.Files);
            }

            if (string.IsNullOrEmpty(page?.NextFileName))
            {
                break;
            }

            startFileName = page.NextFileName;
            startFileId = page.NextFileId;
        }

        return allFiles;
    }

    //This returns a directoryHandle meaning we still need to open a fileHandle on anything we want to read.
    public async Task<(SMB2Client?, SMBLibrary.NTStatus, ISMBFileStore fileStore, object directoryHandle)> ConnectToNASAsync(string path)
    {
        //1 Connect to NAS via SMB
        logger.Debug("Connecting to NAS: " + appSettings.localNASConfig.description);
        var client = new SMB2Client();
        bool isConnected = client.Connect(IPAddress.Parse(appSettings.localNASConfig.ip), SMBTransportType.DirectTCPTransport);
        if (isConnected)
        {
            NTStatus status = client.Login(String.Empty, appSettings.localNASConfig.username, appSettings.localNASConfig.password);
            if (status == NTStatus.STATUS_SUCCESS)
            {
                var fileStore = client.TreeConnect(appSettings.localNASConfig.shareName, out status);
                if (status == NTStatus.STATUS_SUCCESS)
                {
                    //2 Create a handle to the directory specified in path for example xo-vm-backups\\9F4C2855-9FFB-4D0B-B16D-06A2FD3E6F3E
                    object directoryHandle;
                    FileStatus fileStatus;
                    status = fileStore.CreateFile(out directoryHandle, out fileStatus, path, AccessMask.GENERIC_READ, SMBLibrary.FileAttributes.Directory, ShareAccess.Read, CreateDisposition.FILE_OPEN, CreateOptions.FILE_DIRECTORY_FILE, null);
                    if (status == NTStatus.STATUS_SUCCESS)
                    {
                        logger.Debug("Successfully connected to NAS directory: " + path);

                        //3 Return the SMB2Client
                        return (client, status, fileStore, directoryHandle);
                    }
                    else
                    {
                        logger.Error("Failed to open directory: " + status.ToString());
                    }

                    fileStore.Disconnect();
                }
                else
                {
                    logger.Error("SMB Tree Connect failed: " + status.ToString());
                }
            }
            else
            {
                logger.Error("SMB Login failed: " + status.ToString());
            }

            client.Disconnect();

            return (null, status, null, null);
        }

        return (null, NTStatus.STATUS_DATA_ERROR, null, null);
    }

    public async Task<bool> VMBackupAsync(BucketItem bucket, BackupPlan plan, IStorageClient backblazeClient)
    {
        logger.Warn($"Plan {plan.name} - Type {plan.type} - BucketId {bucket.BucketId} - BucketName: {bucket.BucketName} - FolderLocation: {plan.folderLocation}");

        //1 Connect to NAS via SMB
        (SMB2Client? smbClient, NTStatus status, ISMBFileStore fileStore, object directoryHandle) = await ConnectToNASAsync(plan.folderLocation);
        if (status != NTStatus.STATUS_SUCCESS || smbClient == null)
            throw new Exception("Failed to connect to NAS");

        //2 Now query the NAS directory to list files. We do not need to delete here as that is handled as that is handled by the XenOrchestra hypervisor
        List<QueryDirectoryFileInformation> filesinNAS;
        status = fileStore.QueryDirectory(out filesinNAS, directoryHandle, "*", FileInformationClass.FileDirectoryInformation);
        logger.Info($"Files found in NAS folder:: {filesinNAS?.Count ?? 0}");
        if (status == NTStatus.STATUS_SUCCESS || status == NTStatus.STATUS_NO_MORE_FILES)
        {
            foreach (var file in filesinNAS)
            {
                if (file is FileDirectoryInformation fileInfo)  // Cast to FileDirectoryInformation to access properties
                {
                    if (fileInfo.FileName != "." && fileInfo.FileName != "..") // Skip . and .. entries
                    {
                        string fileType = fileInfo.FileAttributes.HasFlag(SMBLibrary.FileAttributes.Directory) ? "DIR" : "FILE";

                        if (fileType == "FILE")
                        {
                            logger.Debug($"\t {fileInfo.FileName} - {fileType} - Size: {fileInfo.EndOfFile} - Uploaded: {fileInfo.LastWriteTime}");
                        }
                    }
                }
            }
        }
        else
        {
            logger.Error("Failed to query directory: " + status.ToString());
        }
        //2.1 Close the directory handle
        fileStore.CloseFile(directoryHandle);
        smbClient.Disconnect();

        //3 Get the files in the remote BackBlaze bucket
        List<FileItem> filesInBucket = await GetFilesFromBackBlazeAsync(backblazeClient, bucket.BucketId);
        logger.Info($"Files found in BackBlaze bucket: {filesInBucket?.Count ?? 0}");
        foreach (FileItem file in filesInBucket)
        {
            logger.Debug($"\t {file.FileName} - Size: {file.ContentLength} - Uploaded: {file.UploadTimestamp}");
        }

        //4 Match on the FileName to see if it exists in BackBlaze already. Check if the upload date is different. If newer upload file.
        var bucketNames = filesInBucket.Select(f => f.FileName).ToHashSet(StringComparer.Ordinal);
        logger.Info("Checking NAS files against BackBlaze by filename...");
        foreach (var fileinNAS in filesinNAS)
        {
            if (fileinNAS is not FileDirectoryInformation nasFile) continue;
            if (nasFile.FileName is "." or "..") continue;
            if (nasFile.FileAttributes.HasFlag(SMBLibrary.FileAttributes.Directory))
                continue;

            if (bucketNames.Contains(nasFile.FileName))
            {
                logger.Debug($"\t Skip (already exists): {nasFile.FileName}");
                continue;
            }

            logger.Debug($"\t Upload (missing in BackBlaze): {nasFile.FileName}");
            if (!appSettings.testMode)
                await StartUploadAsync(nasFile, plan, bucket, backblazeClient);

            // Optional: prevent double-upload within the same run if the NAS list contains duplicates for any reason.
            bucketNames.Add(nasFile.FileName);
        }

        return true;
    }

    public async Task<bool> SQLBackupAsync(BucketItem bucket, BackupPlan plan, IStorageClient backblazeClient)
    {
        logger.Warn($"Plan {plan.name} - Type {plan.type} - BucketId {bucket.BucketId} - BucketName: {bucket.BucketName} - FolderLocation: {plan.folderLocation}");

        //1 Connect to NAS via SMB
        (SMB2Client? smbClient, NTStatus status, ISMBFileStore fileStore, object directoryHandle) = await ConnectToNASAsync(plan.folderLocation);
        if (status != NTStatus.STATUS_SUCCESS || smbClient == null)
            throw new Exception("Failed to connect to NAS");

        //2 Now query the NAS directory to list files. We do not need to delete here as that is handled from the SQL agent.
        List<QueryDirectoryFileInformation> filesinNAS;
        status = fileStore.QueryDirectory(out filesinNAS, directoryHandle, "*", FileInformationClass.FileDirectoryInformation);
        logger.Info($"Files found in NAS folder:: {filesinNAS?.Count ?? 0}");
        if (status == NTStatus.STATUS_SUCCESS || status == NTStatus.STATUS_NO_MORE_FILES)
        {
            Parallel.ForEach(filesinNAS, parallelOptions, file =>
            {
                if (file is FileDirectoryInformation fileInfo)  // Cast to FileDirectoryInformation to access properties
                {
                    if (fileInfo.FileName != "." && fileInfo.FileName != "..") // Skip . and .. entries
                    {
                        string fileType = fileInfo.FileAttributes.HasFlag(SMBLibrary.FileAttributes.Directory) ? "DIR" : "FILE";

                        if (fileType == "FILE")
                        {
                            logger.Debug($"\t {fileInfo.FileName} - {fileType} - Size: {fileInfo.EndOfFile} - Uploaded: {fileInfo.LastWriteTime}");
                        }
                    }
                }
            });
        }
        else
        {
            logger.Error("Failed to query directory: " + status.ToString());
        }
        //2.1 Close the directory handle
        fileStore.CloseFile(directoryHandle);
        smbClient.Disconnect();

        //3 Get the files in the remote BackBlaze bucket
        List<FileItem> filesInBucket = await GetFileVersionsFromBackBlazeAsync(backblazeClient, bucket.BucketId);
        logger.Info($"Files found in BackBlaze bucket: {filesInBucket?.Count ?? 0}");
        Parallel.ForEach(filesInBucket, parallelOptions, file =>
        {
            logger.Debug($"\t {file.FileName} - Size: {file.ContentLength} - Uploaded: {file.UploadTimestamp}");
        });

        //4 Match on the FileName to see if it exists in BackBlaze already. Check if the upload date is different. If newer upload file.
        var bucketNames = filesInBucket.Select(f => f.FileName).ToHashSet(StringComparer.Ordinal);
        logger.Warn("Checking NAS files against BackBlaze by filename...");
        Parallel.ForEach(filesinNAS, parallelOptions, async fileinNAS =>
        {
            if (fileinNAS is not FileDirectoryInformation nasFile) return;
            if (nasFile.FileName is "." or "..") return;
            if (nasFile.FileAttributes.HasFlag(SMBLibrary.FileAttributes.Directory))
                return;

            if (bucketNames.Contains(nasFile.FileName))
            {
                logger.Info($"\t Skip (already exists): {nasFile.FileName}");
                return;
            }

            logger.Info($"\t Upload (missing in BackBlaze): {nasFile.FileName}");
            if (!appSettings.testMode)
                await StartUploadAsync(nasFile, plan, bucket, backblazeClient);

            // Optional: prevent double-upload within the same run if the NAS list contains duplicates for any reason.
            bucketNames.Add(nasFile.FileName);
        });

        //5 Delete files uploaded over 5 days ago only if there are 5 days worth of files uploaded so we delete everything in case backups stopped working
        List<FileItem> filesToDelete = new List<FileItem>();
        int filesWithinRetentionPeriod = 0;
        Parallel.ForEach(filesInBucket, parallelOptions, async file =>
        {
            if (file.UploadTimestamp < DateTime.UtcNow.AddDays(-plan.DailyRetentionDays))
            {
                logger.Warn($"Delete old files: {file.FileName} uploaded on {file.UploadTimestamp}");
                filesToDelete.Add(file);
            }
            else
            {
                logger.Warn($"File within retention: {file.FileName} uploaded on {file.UploadTimestamp}");
                filesWithinRetentionPeriod++;
            }
        });

        if (filesWithinRetentionPeriod >= (plan.DailyRetentionDays-1) * 4) //If the retention period is 5 we want that multiplied by the amount of different files, in this case TLS.bak TLS.trn WMS.bak WMS.trn
        {
            logger.Warn($"Files in retention {filesWithinRetentionPeriod}. Proceeding to delete old files.");
            if (!appSettings.testMode){
                Parallel.ForEach(filesToDelete, parallelOptions, async file => {
                    await backblazeClient.Files.DeleteAsync(file.FileId, file.FileName);
                });
            }
        }

        return true;
    }

    public async Task<bool> QuickBooksBackupAsync(BucketItem bucket, BackupPlan plan, IStorageClient backblazeClient)
    {
        logger.Warn($"Plan {plan.name} - Type {plan.type} - BucketId {bucket.BucketId} - BucketName: {bucket.BucketName} - FolderLocation: {plan.folderLocation}");

        //1 Connect to NAS via SMB
        (SMB2Client? smbClient, NTStatus status, ISMBFileStore fileStore, object directoryHandle) = await ConnectToNASAsync(plan.folderLocation);
        if (status != NTStatus.STATUS_SUCCESS || smbClient == null)
            throw new Exception("Failed to connect to NAS");

        //2 Now query the NAS directory to list files.
        //TODO We will want to make a delete script here but it's low priority since the QB backup is only done monthly anyway,
        List<QueryDirectoryFileInformation> filesinNAS;
        status = fileStore.QueryDirectory(out filesinNAS, directoryHandle, "*", FileInformationClass.FileDirectoryInformation);
        logger.Info($"Files found in NAS folder:: {filesinNAS?.Count ?? 0}");
        if (status == NTStatus.STATUS_SUCCESS || status == NTStatus.STATUS_NO_MORE_FILES)
        {
            foreach (var file in filesinNAS)
            {
                if (file is FileDirectoryInformation fileInfo)  // Cast to FileDirectoryInformation to access properties
                {
                    if (fileInfo.FileName != "." && fileInfo.FileName != "..") // Skip . and .. entries
                    {
                        string fileType = fileInfo.FileAttributes.HasFlag(SMBLibrary.FileAttributes.Directory) ? "DIR" : "FILE";

                        if (fileType == "FILE")
                        {
                            logger.Debug($"\t {fileInfo.FileName} - {fileType} - Size: {fileInfo.EndOfFile} - Uploaded: {fileInfo.LastWriteTime}");
                        }
                    }
                }
            }
        }
        else
        {
            logger.Error("Failed to query directory: " + status.ToString());
        }
        //2.1 Close the directory handle
        fileStore.CloseFile(directoryHandle);
        smbClient.Disconnect();

        //3 Get the files in the remote BackBlaze bucket
        List<FileItem> filesInBucket = await GetFilesFromBackBlazeAsync(backblazeClient, bucket.BucketId);
        logger.Info($"Files found in BackBlaze bucket: {filesInBucket?.Count ?? 0}");
        foreach (FileItem file in filesInBucket)
        {
            logger.Debug($"\t {file.FileName} - Size: {file.ContentLength} - Uploaded: {file.UploadTimestamp}");
        }

        //4 Match on the FileName to see if it exists in BackBlaze already. Check if the upload date is different. If newer upload file.
        var bucketNames = filesInBucket.Select(f => f.FileName).ToHashSet(StringComparer.Ordinal);
        logger.Info("Checking NAS files against BackBlaze by filename...");
        foreach (var fileinNAS in filesinNAS)
        {
            if (fileinNAS is not FileDirectoryInformation nasFile) continue;
            if (nasFile.FileName is "." or "..") continue;
            if (nasFile.FileAttributes.HasFlag(SMBLibrary.FileAttributes.Directory))
                continue;

            if (bucketNames.Contains(nasFile.FileName))
            {
                logger.Debug($"\t Skip (already exists): {nasFile.FileName}");
                continue;
            }

            logger.Debug($"\t Upload (missing in BackBlaze): {nasFile.FileName}");
            if (!appSettings.testMode)
                await StartUploadAsync(nasFile, plan, bucket, backblazeClient);

            // Optional: prevent double-upload within the same run if the NAS list contains duplicates for any reason.
            bucketNames.Add(nasFile.FileName);
        }

        return true;
    }

    public async Task<bool> StartUploadAsync(FileDirectoryInformation fileInfoNAS, BackupPlan plan, BucketItem bucket, IStorageClient backblazeClient)
    {
        //1 Reconnect to NAS to get the file stream. A new instance of SMB2Client is needed? How are we going to handle doing this in parallel? 
        (SMB2Client? smbClient2, NTStatus status2, ISMBFileStore fileStore2, object directoryHandle2) = await ConnectToNASAsync(plan.folderLocation);
        if (status2 != NTStatus.STATUS_SUCCESS || smbClient2 == null)
            throw new Exception("Failed to connect to NAS");

        //2 Validate the file size against maxFileSizeToUploadInGB setting
        bool validate1 = await validateMaxFileSizeToUploadInGB(fileInfoNAS.EndOfFile);
        if (!validate1)
            throw new Exception("File size exceeds maxFileSizeToUploadInGB setting");

        //3 Determine if we need to do a simple upload or a multiPart upload based on chunkSizeInMB setting
        bool multiPart = await validateChunkSizeInMB(fileInfoNAS.EndOfFile);
        string filePath = plan.folderLocation + "\\" + fileInfoNAS.FileName;
        if (multiPart == false)
        {
            logger.Debug($"\t File size is less than chunkSizeInMB. Will do a simple upload.");

            //4.0 Open the fileHandle for reading. This is different from opening a directory. The handle is different.
            object fileHandle;
            FileStatus fileStatus;
            var openStatus = fileStore2.CreateFile(out fileHandle, out fileStatus, filePath, AccessMask.GENERIC_READ, SMBLibrary.FileAttributes.Normal,
                ShareAccess.Read, CreateDisposition.FILE_OPEN, CreateOptions.FILE_NON_DIRECTORY_FILE, null);
            if (openStatus != NTStatus.STATUS_SUCCESS)
                throw new Exception($"Failed to open file '{fileInfoNAS.FileName}' for reading: {openStatus}");

            // 4.1 and 4.2 Read file in small 1mb SMB chunks but buffer to memory
            using var fullStream = await ReadFileInChunks(fileStore2, fileHandle, fileInfoNAS);

            //4.3 Upload to Backblaze
            var results = await backblazeClient.UploadAsync(bucket.BucketId, fileInfoNAS.FileName, fullStream);
            logger.Warn($"Simple file upload finished: {results.Response?.FileName}");
        }
        else //MultiPart upload
        {
            logger.Debug($"\t File size exceeds chunkSizeInMB. Will do a large file upload.");

            //4.0 Open the fileHandle for reading. This is different from opening a directory. The handle is different.
            object fileHandle;
            FileStatus fileStatus;
            var openStatus = fileStore2.CreateFile(out fileHandle, out fileStatus, filePath,
                AccessMask.GENERIC_READ, SMBLibrary.FileAttributes.Normal, ShareAccess.Read, CreateDisposition.FILE_OPEN, CreateOptions.FILE_NON_DIRECTORY_FILE, null);
            if (openStatus != NTStatus.STATUS_SUCCESS)
                throw new Exception($"Failed to open file '{fileInfoNAS.FileName}' for reading: {openStatus}");

            //4.1 Start the Large File upload by getting a FileId from BackBlaze
            var resultsStartLargeFile = await backblazeClient.Parts.StartLargeFileAsync(bucket.BucketId, fileInfoNAS.FileName);
            var fileId = resultsStartLargeFile.Response.FileId;
            logger.Debug("Large file upload started. FileId: " + fileId);

            int partSize = appSettings.chunkSizeInMB * 1024 * 1024; // 100 MB
            long offset = 0;
            int partNumber = 1;
            var sha1List = new List<string>();

            using var backblazeChunkBuffer = new MemoryStream();

            //4.2 Carve out 100mb chunks from the file on the NAS and upload each chunk to BackBlaze
            while (offset < fileInfoNAS.EndOfFile)
            {
                //4.2.1 Read from NAS via SMB at specific offset
                int bytesToRead = (int)Math.Min(appSettings.smbChunkSize, fileInfoNAS.EndOfFile - offset);
                NTStatus readStatus = fileStore2.ReadFile(out byte[] chunkData, fileHandle, offset, bytesToRead);
                if (readStatus != NTStatus.STATUS_SUCCESS)
                    throw new Exception($"Read failed at offset {offset}: {readStatus}");

                //4.2.2 Write 1mb SMB chunk to Backblaze buffer
                if (chunkData?.Length > 0)
                {
                    // Add SMB chunk to Backblaze buffer
                    await backblazeChunkBuffer.WriteAsync(chunkData, 0, chunkData.Length);
                    offset += chunkData.Length;

                    // Progress reporting for SMB reads (every 10MB or at completion)
                    if (offset % (10 * 1024 * 1024) == 0 || offset == fileInfoNAS.EndOfFile)
                    {
                        double progress = (double)offset / fileInfoNAS.EndOfFile * 100;
                        logger.Debug($"\tSMB reading progress: {progress:F1}% ({offset:N0}/{fileInfoNAS.EndOfFile:N0} bytes)");
                    }
                }

                // When buffer reaches 100MB OR we've read the entire file, upload to Backblaze
                bool bufferFull = backblazeChunkBuffer.Length >= partSize;
                bool fileComplete = offset >= fileInfoNAS.EndOfFile;
                if (bufferFull || fileComplete)
                {
                    if (backblazeChunkBuffer.Length > 0)
                    {
                        //4.2.3 Get upload URL for the part. This is because we need an authTokken to upload each part.
                        var uploadUrlResult = await backblazeClient.Parts.GetUploadUrlAsync(fileId);
                        Uri uploadUrl = uploadUrlResult.Response.UploadUrl;
                        string authToken = uploadUrlResult.Response.AuthorizationToken;

                        //4.2.4 Upload to Backblaze
                        backblazeChunkBuffer.Position = 0;
                        var uploadPartResult = await backblazeClient.Parts.UploadAsync(uploadUrl, partNumber, authToken, backblazeChunkBuffer, null);
                        logger.Info($"Uploaded part {partNumber} of file: {fileInfoNAS.FileName}");
                        sha1List.Add(uploadPartResult.Response.ContentSha1);

                        backblazeChunkBuffer.SetLength(0);
                        backblazeChunkBuffer.Position = 0;
                        partNumber++;

                        if (fileComplete)
                            break; // Exit the loop if we've completed the file
                    }
                }
            }

            //4.3 Finish the large file upload
            var resultsFinishLargeFile = await backblazeClient.Parts.FinishLargeFileAsync(fileId, sha1List);
            logger.Warn($"Large file upload finished: {resultsFinishLargeFile.Response.FileName}");
        }

        //5 Close the SMB connection
        smbClient2.Disconnect();

        return true;
    }
    
    //This is because SMB can only read so much data at a time. We read in 1mb chunks and write to a memory buffer until the whole file is read.
    private async Task<MemoryStream> ReadFileInChunks(ISMBFileStore fileStore, object fileHandle, FileDirectoryInformation fileInfo)
    {
        long totalBytesRead = 0;
        long fileSize = fileInfo.EndOfFile;
        var memoryStream = new MemoryStream((int)fileSize); // Pre-allocate memory

        logger.Debug($"\tReading {fileInfo.FileName} ({fileSize:N0} bytes) in {appSettings.smbChunkSize:N0} byte SMB chunks...");

        while (totalBytesRead < fileSize)
        {
            int bytesToRead = (int)Math.Min(appSettings.smbChunkSize, fileSize - totalBytesRead);

            NTStatus readStatus = fileStore.ReadFile(out byte[] chunkData, fileHandle, totalBytesRead, bytesToRead);

            if (readStatus != NTStatus.STATUS_SUCCESS)
            {
                memoryStream.Dispose();
                throw new Exception($"Failed to read SMB chunk at offset {totalBytesRead}: {readStatus}");
            }

            if (chunkData == null || chunkData.Length == 0)
                break;

            // Write SMB chunk to memory buffer
            await memoryStream.WriteAsync(chunkData, 0, chunkData.Length);
            totalBytesRead += chunkData.Length;

            // Progress reporting
            if (totalBytesRead % (10 * 1024 * 1024) == 0 || totalBytesRead == fileSize)
            {
                double progress = (double)totalBytesRead / fileSize * 100;
                logger.Debug($"\tSMB reading progress: {progress:F1}% ({totalBytesRead:N0}/{fileSize:N0} bytes)");
            }
        }

        memoryStream.Position = 0; // Reset for reading
        logger.Debug($"\tSuccessfully buffered {totalBytesRead:N0} bytes in memory for Backblaze upload");
        return memoryStream;
    }

}