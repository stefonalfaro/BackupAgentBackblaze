# QuickBooks Backup Synchronization Script
# Compares recent .qbb files and copies missing ones to backup location on the NAS

# Define paths and credentials
$sourcePath = "\\server\Quickbooks\Backup"
$destinationDrive = "Z:"
$destinationUNC = "\\192.168.2.10\backups\quickbooks-backups"
$username = "192.168.2.10\Backups"
$password = ""
$searchString = @("express inc. 2015", "Logistics Inc.", "ONTARIO INC.")
$fileExtension = "*.qbb"

# Define log file path (same directory as script)
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$logFile = Join-Path $scriptDir "QuickBooksBackupSync_$(Get-Date -Format 'yyyyMMdd').log"

# Function to write timestamped log messages to file and console
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] [$Level] $Message"
    
    # Write to both console and file
    Write-Host $logEntry
    Add-Content -Path $logFile -Value $logEntry
}

try {
    Write-Log "Starting QuickBooks backup synchronization script"
    Write-Log "Log file: $logFile"
    
    # Check if source path exists
    if (-not (Test-Path $sourcePath)) {
        throw "Source path not accessible: $sourcePath"
    }
    
    Write-Log "Searching for .qbb files containing search patterns in $sourcePath"
    
    # Initialize array to collect all matching files
    $allSourceFiles = @()
    
    # Search for files matching each pattern (10 most recent per pattern)
    foreach ($pattern in $searchString) {
        Write-Log "Searching for files containing: '$pattern'"
        
        $patternFiles = Get-ChildItem -Path $sourcePath -Filter $fileExtension -File | 
            Where-Object { $_.Name -like "*$pattern*" } | 
            Sort-Object LastWriteTime -Descending | 
            Select-Object -First 10
        
        Write-Log "  Found $($patternFiles.Count) files matching '$pattern'"
        foreach ($file in $patternFiles) {
            Write-Log "    - $($file.Name) (Modified: $($file.LastWriteTime))"
        }
        
        # Add to master collection
        $allSourceFiles += $patternFiles
    }
    
    # Remove duplicates (in case a file matches multiple patterns) and sort by date
    $sourceFiles = $allSourceFiles | Sort-Object FullName -Unique | Sort-Object LastWriteTime -Descending
    
    if ($sourceFiles.Count -eq 0) {
        Write-Log "No .qbb files found matching any search patterns" "WARNING"
        exit 0
    }
    
    Write-Log "Total unique files found: $($sourceFiles.Count)"
    
    # Check if drive is already mounted and unmount if necessary
    $existingMount = net use | Where-Object { $_ -match "$destinationDrive" }
    if ($existingMount) {
        Write-Log "Drive $destinationDrive is already mounted, attempting to disconnect first"
        try {
            Invoke-Expression "net use $destinationDrive /delete /y" 2>&1 | Out-Null
            Start-Sleep -Seconds 2
        } catch {
            Write-Log "Warning: Could not unmount existing drive, continuing anyway" "WARNING"
        }
    }
    
    # Mount the destination drive
    Write-Log "Mounting destination drive $destinationDrive from $destinationUNC"
    $netUseCommand = "net use $destinationDrive $destinationUNC /user:$username $password"
    $result = Invoke-Expression $netUseCommand 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        # Check if it failed because drive is already connected to same location
        if ($result -match "multiple connections|already connected") {
            Write-Log "Drive already connected to destination, continuing" "WARNING"
        } else {
            throw "Failed to mount destination drive. Error: $result"
        }
    } else {
        Write-Log "Successfully mounted $destinationDrive"
    }
    
    # Check if destination path is accessible
    if (-not (Test-Path $destinationDrive)) {
        throw "Destination drive $destinationDrive is not accessible after mounting"
    }
    
    # Compare files and copy missing ones
    $copiedCount = 0
    $skippedCount = 0
    
    Write-Log "Comparing files and copying missing ones..."
    
    foreach ($sourceFile in $sourceFiles) {
        $destinationFilePath = Join-Path $destinationDrive $sourceFile.Name
        
        if (Test-Path $destinationFilePath) {
            # File exists, check if sizes match (basic comparison)
            $destFile = Get-Item $destinationFilePath
            if ($sourceFile.Length -eq $destFile.Length -and $sourceFile.LastWriteTime -eq $destFile.LastWriteTime) {
                Write-Log "  SKIP: $($sourceFile.Name) (already exists with same size and date)"
                $skippedCount++
            } else {
                Write-Log "  COPY: $($sourceFile.Name) (different size or date, overwriting)"
                Copy-Item $sourceFile.FullName $destinationFilePath -Force
                $copiedCount++
            }
        } else {
            Write-Log "  COPY: $($sourceFile.Name) (missing from destination)"
            Copy-Item $sourceFile.FullName $destinationFilePath
            $copiedCount++
        }
    }
    
    Write-Log "File comparison complete. Copied: $copiedCount, Skipped: $skippedCount"
    
} catch {
    Write-Log "ERROR: $($_.Exception.Message)" "ERROR"
    $errorOccurred = $true
} finally {
    # Always attempt to unmount the drive
    Write-Log "Unmounting network drive..."
    try {
        # Try to unmount the specific drive letter first
        $unmountResult = Invoke-Expression "net use $destinationDrive /delete /y" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Log "Successfully unmounted $destinationDrive"
        } else {
            # If that fails, try the broader IP-based unmount
            Write-Log "Trying alternate unmount method..." "WARNING"
            $unmountResult2 = Invoke-Expression "net use \\192.168.2.10 /delete /y" 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Log "Successfully unmounted via IP address"
            } else {
                Write-Log "Warning: Could not unmount drive - $unmountResult2" "WARNING"
            }
        }
    } catch {
        Write-Log "Warning: Exception during unmount - $($_.Exception.Message)" "WARNING"
    }
    
    if ($errorOccurred) {
        Write-Log "Script completed with errors" "ERROR"
        exit 1
    } else {
        Write-Log "Script completed successfully"
        exit 0
    }
}