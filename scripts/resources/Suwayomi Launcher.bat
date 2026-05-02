@echo off
setlocal EnableExtensions

set "REPO_OWNER=xkana-shii"
set "REPO_NAME=Suwayomi-Launcher"
set "LAUNCHER_JAR=Suwayomi-Launcher.jar"

cd /d "%~dp0" || exit /b 1

if not exist "jre\bin\javaw.exe" (
    echo ERROR: JRE not found at jre\bin\javaw.exe
    exit /b 1
)

set "PS_SCRIPT=%TEMP%\SuwayomiUpdater-%RANDOM%.ps1"

(
echo $ErrorActionPreference = 'Stop'
echo $owner    = '%REPO_OWNER%'
echo $repo     = '%REPO_NAME%'
echo $launcher = '%LAUNCHER_JAR%'
echo $headers  = @{ 'User-Agent' = 'Suwayomi-Launcher-Updater' }
echo $localVer = ''
echo if (Test-Path -LiteralPath $launcher) {
echo     Add-Type -AssemblyName System.IO.Compression.FileSystem
echo     $zip = [System.IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $launcher))
echo     try {
echo         $entry = $zip.GetEntry('META-INF/MANIFEST.MF')
echo         if ($entry) {
echo             $reader = [System.IO.StreamReader]::new($entry.Open())
echo             try   { $manifest = $reader.ReadToEnd() }
echo             finally { $reader.Dispose() }
echo             $match = [regex]::Match($manifest, '(?m)^Implementation-Version:\s*(.+?)\s*$')
echo             if ($match.Success) { $localVer = $match.Groups[1].Value.Trim() }
echo         }
echo     } finally { $zip.Dispose() }
echo }
echo $release = Invoke-RestMethod -Uri ('https://api.github.com/repos/{0}/{1}/releases/latest' -f $owner, $repo) -Headers $headers
echo if ($localVer -and $localVer -eq $release.tag_name -and (Test-Path -LiteralPath $launcher)) { exit 0 }
echo $asset = $release.assets ^| Where-Object name -like '*.jar' ^| Select-Object -First 1
echo if (-not $asset) { throw 'No .jar asset found in latest release.' }
echo if (Test-Path -LiteralPath $launcher) {
echo     try {
echo         $fs = [System.IO.File]::Open($launcher, 'Open', 'ReadWrite', 'None')
echo         $fs.Dispose()
echo     } catch {
echo         throw 'Launcher JAR is locked. Close the launcher before updating.'
echo     }
echo }
echo $tmp = Join-Path $env:TEMP ('Suwayomi-Launcher-{0}.jar' -f [guid]::NewGuid().ToString('N'))
echo Invoke-WebRequest -Uri $asset.browser_download_url -Headers $headers -OutFile $tmp -UseBasicParsing
echo if ((Get-Item $tmp).Length -lt 10000) {
echo     Remove-Item -LiteralPath $tmp -Force
echo     throw 'Downloaded file is suspiciously small. Aborting update.'
echo }
echo Move-Item -LiteralPath $tmp -Destination $launcher -Force
) > "%PS_SCRIPT%"

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%"
set "PS_EXIT=%ERRORLEVEL%"

del /f /q "%PS_SCRIPT%" 2>nul

if %PS_EXIT% neq 0 exit /b %PS_EXIT%

start "" "jre\bin\javaw.exe" --add-exports=java.desktop/sun.awt=ALL-UNNAMED -jar "%LAUNCHER_JAR%" %*
exit /b 0
