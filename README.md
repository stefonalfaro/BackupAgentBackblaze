# Large File Backup Using SMB and Backblaze Partial Uploads

This application provides a robust solution for backing up large files from SMB network shares directly to Backblaze B2 cloud storage, without requiring local disk space for the full file. It leverages a combination of network carving techniques and Backblaze's multipart upload APIs to efficiently handle very large files.

<img width="759" height="516" alt="image" src="https://github.com/user-attachments/assets/7e156bc3-dcbb-4de8-b46e-1e6035a7e92c" />

---

## Key Concepts

### Network Carving via SMB Protocol

Rather than downloading the entire file locally before uploading, this solution reads large files **in chunks directly over the network** using the SMB2 protocol. This "network carving" approach reads fixed-size byte ranges from the NAS or SMB share, minimizing local memory and disk usage.

### Backblaze Partial (Multipart) Uploads

Backblaze B2 supports multipart uploads, allowing files to be uploaded in discrete parts. This fits perfectly with chunked reading over SMB, enabling the application to upload each file chunk immediately after reading it, streaming data efficiently without needing the entire file in memory.

By combining these two:

- Files larger than the configured chunk size are split and read from the NAS over SMB in manageable parts.
- Each part is uploaded immediately to Backblaze, enabling scalable and memory-efficient backups.
- For small files, a simple single upload is performed.

---

## Configuration (`appsettings.json`)

The application is highly configurable via the `appsettings.json` file. Below is an example configuration with explanations:

```json
{
  "AppSettings": {
    "testMode": true,
    "chunkSizeInMB": 100,
    "backblazeConfig": {
      "keyId": "",
      "applicationKey": "",
      "description": "GAPP Backblaze Account for Cloud Storage"
    },
    "localNASConfig": {
      "ip": "192.168.2.10",
      "description": "UniFi UNAS Pro",
      "connectionType": "SMB",
      "username": "",
      "password": "",
      "shareName": "backups"
    },
    "backupPlans": [
      {
        "name": "Development Plan for Testing",
        "bucketId": "6ebe6048684387ac9d48061c",
        "type": "VM",
        "vmOperation": {
          "GUID": "399dcc39-cffe-3acd-96ce-59f4c9169b79",
          "fileType": "XVA",
          "folderLocation": "xo-vm-backups\\399dcc39-cffe-3acd-96ce-59f4c9169b79"
        },
        "DailyRetentionDays": 3,
        "WeeklyRetentionDays": 0,
        "MonthlyRetentionDays": 1
      }
    ]
  }
}
```

This program takes advantage of how both the SMB remote file protocol and BackBlaze B2 Storage API support partial read/write. 

The local NAS on the GAPP network supports SMB protocol meaning we can carve out bytes from a file using the offset and byte size.

BackBlaze HTTP API supports partial uploads, meaning we can upload the first 100mb chunk and give it an id of 1, do the next 100mb and tag it id 2, then close the file and have BackBlaze reassmble the full 200mb file.

The SMB ReadFile lets us carve out chunks of the file across the network. So we can get 100mb chunks that we can load into memory and then use BackBlaze multi part upload to upload each 100mb part at a time. This lets us go directly from the SMB NAS to BackBlaze Cloud without needing any additional local sotrage. We can assume 1Gbit connection to NAS although there is a 10Gbit link as well I'm just not sure if the servers have it. The WAN upload will be 100mbit.

You can stream 100MB chunks directly from a file on the NAS over SMB to Backblaze without loading the full file. SMB allows random access reads (seek and read specific byte ranges). As long as you're okay with slightly slower transfers and more complex error handling, it's a clean and scalable approach.

## Developer Notes
SMB2Client should be created per operation (not singleton) → you’re doing this correctly.

IStorageClient (your Backblaze client) can be reused safely (singleton) → also correct.

As each VMBackupAsync call handles its own state (no shared file handles, no global data), then you can safely run multiple operations in parallel.

## Automatic SQL Backups to NAS
We have .BAK and .TRN files on the NAS in the sql-backups folder.
SQL Server Agent lets us mount the NAS ``net use Z: \\192.168.2.10\backups\sql-backups /user:192.168.2.10\Backups PASSWORDHERE`` which means our regular backup script now points to Z:\ instead.
Then at the end we should clear it ``net use \\192.168.2.10 /delete /y``

## XenOrchestra Virtual Machines to NAS
We have .XVA files on the NAS in the xo-vm-backups folders.

## Automatic Quckbooks Backups to NAS
There already is a Quickbooks backup process that runs locally. We now need to get this folder or specific files off the main working fileshare and over to the NAS.

## Windows Start
Wrap this in a batch or powershell ``dotnet BackupAgentBackblaze.dll``

## Linux Start
``dotnet BackupAgentBackblaze.dll``
