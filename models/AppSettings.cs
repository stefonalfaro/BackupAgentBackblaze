public class AppSettings
{
    public bool testMode { get; set; } = false;
    public int smbChunkSize { get; set; } = 1024*1024; //1 MB default SMB read chunk size
    public int threadCount { get; set; } = 4; //Default to 4 threads for parallel uploads
    public int chunkSizeInMB { get; set; } = 100; //100 MB default. The whole point is so we can carve out chunks of a remote network SMB file to partially upload in parts to BackBlaze.
    public int maxFileSizeToUploadInGB { get; set; } = 1000; //1000 GB default (gappserver3 is 800gb)
    public BackblazeConfig backblazeConfig { get; set; }
    public LocalNASConfig localNASConfig { get; set; }
    public BackupPlan[] backupPlans { get; set; }
}

public class BackblazeConfig
{
    public string keyId { get; set; }
    public string applicationKey { get; set; }
    public string description { get; set; }
}

public class LocalNASConfig
{
    public string description { get; set; }
    public string ip { get; set; }
    public string connectionType { get; set; } //SMB, NFS
    public string username { get; set; }
    public string password { get; set; }
    public string shareName { get; set; }
}

public class BackupPlan
{
    public bool enabled { get; set; } = true;
    public string name { get; set; }
    public string bucketId { get; set; } //BackBlaze Bucket ID
    public bool encryption { get; set; } = false; //Encryption at Rest
    public string folderLocation { get; set; } //Folder on NAS to backup from
    public BackupPlanType type { get; set; } //VM, SQL, QuickBooks
    public int DailyRetentionDays { get; set; }
    public int WeeklyRetentionDays { get; set; }
    public int MonthlyRetentionDays { get; set; }
}

public enum BackupPlanType
{
    VM,
    SQL,
    QuickBooks
}
