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

        logger.Debug($"Thread Count is set to {appSettings.threadCount}");

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
        if (mb > appSettings.chunkSizeInMB)
        {
            logger.Debug($"File size {mb} MB exceeds chunkSizeInMB setting of {appSettings.chunkSizeInMB} MB. Will do a large file upload.");
            return false;
        }

        //2 If not exceeded return true
        return true;
    }   

    public async Task<List<FileItem>> GetFilesFromBackBlazeAsync(IStorageClient client, string BucketId)
    {
        IApiResults<ListFileNamesResponse> response = await client.Files.ListNamesAsync(BucketId);
        ListFileNamesResponse responseObject = response.Response;
        List<FileItem> files = responseObject.Files;

        return files;
    }

    //This returns a directoryHandle meaning we still need to open a fileHandle on anything we want to read.
    public async Task<(SMB2Client?, SMBLibrary.NTStatus, ISMBFileStore fileStore, object directoryHandle)> ConnectToNASAsync(string path)
    {
        //1 Connect to NAS via SMB
        logger.Info("Connecting to NAS: " + appSettings.localNASConfig.description);
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
                        logger.Info("Successfully connected to NAS directory: " + path);

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
        logger.Warn($"Plan {plan.name} - Type {plan.type} - BucketId {bucket.BucketId} - BucketName: {bucket.BucketName} - VM GUID: {plan.vmOperation.GUID} - FileType: {plan.vmOperation.fileType} - FolderLocation: {plan.folderLocation}");

        //1 Connect to NAS via SMB
        (SMB2Client? smbClient, NTStatus status, ISMBFileStore fileStore, object directoryHandle) = await ConnectToNASAsync(plan.folderLocation);
        if (status != NTStatus.STATUS_SUCCESS || smbClient == null)
            throw new Exception("Failed to connect to NAS");

        //2 Now query the NAS directory to list files
        List<QueryDirectoryFileInformation> filesinNAS;
        status = fileStore.QueryDirectory(out filesinNAS, directoryHandle, "*", FileInformationClass.FileDirectoryInformation);
        Console.WriteLine($"Files found in NAS folder:: {filesinNAS?.Count ?? 0}");
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
                            Console.WriteLine($"\t {fileInfo.FileName} - {fileType} - Size: {fileInfo.EndOfFile} - Uploaded: {fileInfo.LastWriteTime}");
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
        Console.WriteLine($"Files found in BackBlaze bucket: {filesInBucket?.Count ?? 0}");
        foreach (FileItem file in filesInBucket)
        {
            Console.WriteLine($"\t {file.FileName} - Size: {file.ContentLength} - Uploaded: {file.UploadTimestamp}");
        }

        //4 Match on the FileName to see if it exists in BackBlaze already. Check if the upload date is different. If newer upload file.
        foreach (var fileinNAS in filesinNAS)
        {
            if (fileinNAS is FileDirectoryInformation fileInfoNAS)  // Cast to FileDirectoryInformation to access properties
            {
                foreach (FileItem fileinBucket in filesInBucket)
                {
                    bool uploadNeeded = false;

                    if (fileInfoNAS.FileName == fileinBucket.FileName)
                    {
                        //4.1 Compare the LastWriteTime from NAS to the UploadTimestamp from BackBlaze to determine if the file needs to be uploaded.
                        if (fileInfoNAS.LastWriteTime > fileinBucket.UploadTimestamp)
                        {
                            Console.WriteLine($"\t NAS File: {fileInfoNAS.FileName} is newer than BackBlaze File: {fileinBucket.FileName}. Needs to be uploaded.");
                            uploadNeeded = true;
                        }
                        else
                        {
                            Console.WriteLine($"\t NAS File: {fileInfoNAS.FileName} is NOT newer than BackBlaze File: {fileinBucket.FileName}. No upload needed.");
                        }
                    }
                    else //File doesnt exist yet so yes we can upload it.
                    {
                        Console.WriteLine($"\t NAS File: {fileInfoNAS.FileName} does not exist in BackBlaze. Needs to be uploaded.");
                        uploadNeeded = true;
                    }

                    if (uploadNeeded)
                    {
                       await StartUploadAsync(fileInfoNAS, plan, bucket, backblazeClient, directoryHandle);
                    }
                }
            }
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

        //2 Now query the NAS directory to list files
        List<QueryDirectoryFileInformation> filesinNAS;
        status = fileStore.QueryDirectory(out filesinNAS, directoryHandle, "*", FileInformationClass.FileDirectoryInformation);
        Console.WriteLine($"Files found in NAS folder:: {filesinNAS?.Count ?? 0}");
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
                            Console.WriteLine($"\t {fileInfo.FileName} - {fileType} - Size: {fileInfo.EndOfFile} - Uploaded: {fileInfo.LastWriteTime}");
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
        Console.WriteLine($"Files found in BackBlaze bucket: {filesInBucket?.Count ?? 0}");
        foreach (FileItem file in filesInBucket)
        {
            Console.WriteLine($"\t {file.FileName} - Size: {file.ContentLength} - Uploaded: {file.UploadTimestamp}");
        }

        //4 Match on the FileName to see if it exists in BackBlaze already. Check if the upload date is different. If newer upload file.
        foreach (var fileinNAS in filesinNAS)
        {
            if (fileinNAS is FileDirectoryInformation fileInfoNAS)  // Cast to FileDirectoryInformation to access properties
            {
                foreach (FileItem fileinBucket in filesInBucket)
                {
                    bool uploadNeeded = false;

                    if (fileInfoNAS.FileName == fileinBucket.FileName)
                    {
                        //4.1 Compare the LastWriteTime from NAS to the UploadTimestamp from BackBlaze to determine if the file needs to be uploaded.
                        if (fileInfoNAS.LastWriteTime > fileinBucket.UploadTimestamp)
                        {
                            Console.WriteLine($"\t NAS File: {fileInfoNAS.FileName} is newer than BackBlaze File: {fileinBucket.FileName}. Needs to be uploaded.");
                            uploadNeeded = true;
                        }
                        else
                        {
                            Console.WriteLine($"\t NAS File: {fileInfoNAS.FileName} is NOT newer than BackBlaze File: {fileinBucket.FileName}. No upload needed.");
                        }
                    }
                    else //File doesnt exist yet so yes we can upload it.
                    {
                        Console.WriteLine($"\t NAS File: {fileInfoNAS.FileName} does not exist in BackBlaze. Needs to be uploaded.");
                        uploadNeeded = true;
                    }

                    if (uploadNeeded)
                    {
                        await StartUploadAsync(fileInfoNAS, plan, bucket, backblazeClient, directoryHandle);
                    }
                }
            }
        }

        return true;
    }

    public async Task<bool> StartUploadAsync(FileDirectoryInformation fileInfoNAS, BackupPlan plan, BucketItem bucket, IStorageClient backblazeClient, object directoryHandle)
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
        if (multiPart == false)
        {
            Console.WriteLine($"\t File size is less than chunkSizeInMB. Will do a simple upload.");

            //4.0 Open the fileHandle for reading. This is different from opening a directory. The handle is different.
            object fileHandle;
            FileStatus fileStatus;
            var openStatus = fileStore2.CreateFile(out fileHandle, out fileStatus, fileInfoNAS.FileName, AccessMask.GENERIC_READ, SMBLibrary.FileAttributes.Normal,
                ShareAccess.Read, CreateDisposition.FILE_OPEN, CreateOptions.FILE_NON_DIRECTORY_FILE, null);
            if (openStatus != NTStatus.STATUS_SUCCESS)
                throw new Exception($"Failed to open file '{fileInfoNAS.FileName}' for reading: {openStatus}");

            //4.1  Read the entire file into memory
            NTStatus readStatus = fileStore2.ReadFile(out byte[] fullData, fileHandle, 0, (int)fileInfoNAS.EndOfFile);
            if (readStatus != NTStatus.STATUS_SUCCESS)
                throw new Exception($"Read failed: {readStatus}");

            //4.2 Put the full data into a MemoryStream for upload
            using var fullStream = new MemoryStream(fullData);

            //4.3 Upload to Backblaze
            if (!appSettings.testMode)
            {
                var results = await backblazeClient.UploadAsync(bucket.BucketId, fileInfoNAS.FileName, fullStream);
                Console.WriteLine($"Simple file upload finished: {results.Response?.FileName}");
            }
            else
            {
                Console.WriteLine($"TEST MODE - Skipping actual upload of file: {fileInfoNAS.FileName}");
            }

        }
        else //MultiPart upload
        {
            Console.WriteLine($"\t File size exceeds chunkSizeInMB. Will do a large file upload.");

            //4.0 Open the fileHandle for reading. This is different from opening a directory. The handle is different.
            object fileHandle;
            FileStatus fileStatus;
            var openStatus = fileStore2.CreateFile(out fileHandle, out fileStatus, fileInfoNAS.FileName,
                AccessMask.GENERIC_READ, SMBLibrary.FileAttributes.Normal, ShareAccess.Read, CreateDisposition.FILE_OPEN, CreateOptions.FILE_NON_DIRECTORY_FILE, null);
            if (openStatus != NTStatus.STATUS_SUCCESS)
                throw new Exception($"Failed to open file '{fileInfoNAS.FileName}' for reading: {openStatus}");

            //4.1 Start the Large File upload by getting a FileId from BackBlaze
            var resultsStartLargeFile = await backblazeClient.Parts.StartLargeFileAsync(bucket.BucketId, fileInfoNAS.FileName);
            var fileId = resultsStartLargeFile.Response.FileId;
            Console.WriteLine("Large file upload started. FileId: " + fileId);

            int partSize = appSettings.chunkSizeInMB * 1024 * 1024; // 100 MB
            long offset = 0;
            int partNumber = 1;
            var sha1List = new List<string>();

            //4.2 Carve out 100mb chunks from the file on the NAS and upload each chunk to BackBlaze
            while (offset < fileInfoNAS.EndOfFile)
            {
                //4.2.1 Read from NAS via SMB at specific offset
                int bytesToRead = (int)Math.Min(partSize, fileInfoNAS.EndOfFile - offset);
                NTStatus readStatus = fileStore2.ReadFile(out byte[] chunkData, fileHandle, offset, bytesToRead);
                if (readStatus != NTStatus.STATUS_SUCCESS)
                    throw new Exception($"Read failed at offset {offset}: {readStatus}");

                //4.2.2 Put the chunk data into a MemoryStream for upload
                using var chunkStream = new MemoryStream(chunkData);

                //4.2.3 Get upload URL for the part. This is because we need an authTokken to upload each part.
                var uploadUrlResult = await backblazeClient.Parts.GetUploadUrlAsync(fileId);
                Uri uploadUrl = uploadUrlResult.Response.UploadUrl;
                string authToken = uploadUrlResult.Response.AuthorizationToken;

                //4.2.4 Upload to Backblaze
                if (!appSettings.testMode)
                {
                    var uploadPartResult = await backblazeClient.Parts.UploadAsync(uploadUrl, partNumber, authToken, chunkStream, null);
                    Console.WriteLine($"Uploaded part {partNumber} of file: {fileInfoNAS.FileName}");
                    sha1List.Add(uploadPartResult.Response.ContentSha1);
                }
                else
                {
                    Console.WriteLine($"TEST MODE - Skipping actual upload of part {partNumber} of file: {fileInfoNAS.FileName}");
                    sha1List.Add("TESTMODEFAKESHA1PART" + partNumber.ToString("D4"));  // In test mode, we can simulate a SHA1 for tracking purposes
                }

                offset += bytesToRead;
                partNumber++;
            }

            //4.3 Finish the large file upload
            var resultsFinishLargeFile = await backblazeClient.Parts.FinishLargeFileAsync(fileId, sha1List);
            Console.WriteLine($"Large file upload finished: {resultsFinishLargeFile.Response.FileName}");
            if (appSettings.testMode)
            {
                Console.WriteLine($"TEST MODE - Error intentional due to invalid sha1List. This is expected in test mode.");
            }
        }

        //5 Close the directory handle and SMB connection
        fileStore2.CloseFile(directoryHandle);
        smbClient2.Disconnect();

        return true;
    }

}