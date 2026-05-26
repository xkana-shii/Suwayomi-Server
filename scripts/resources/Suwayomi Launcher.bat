@echo off
setlocal EnableExtensions

set "REPO_OWNER=xkana-shii"
set "REPO_NAME=Suwayomi-Launcher"
set "LAUNCHER_JAR=Suwayomi-Launcher.jar"
set "SERVER_DIR=bin"
set "SERVER_JAR=%SERVER_DIR%\Suwayomi-Server.jar"

cd /d "%~dp0" || exit /b 1

if not exist "jre\bin\javaw.exe" (
  echo ERROR: JRE not found at jre\bin\javaw.exe
  exit /b 1
)

REM --- Ensure bin\Suwayomi-Server.jar exists, using a versioned jar if necessary ---
if not exist "%SERVER_JAR%" (
  for %%f in ("%SERVER_DIR%\Suwayomi-Server-*.jar") do (
    if /I not "%%~nxf"=="Suwayomi-Server.jar" (
      copy "%%f" "%SERVER_JAR%" >nul
      del /q "%SERVER_DIR%\Suwayomi-Server-*.jar"
      goto :found_server_jar
    )
  )
  echo ERROR: Could not find %SERVER_JAR% or any versioned Suwayomi-Server-*.jar in %SERVER_DIR%
  exit /b 1
)
:found_server_jar

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$owner='%REPO_OWNER%'; $repo='%REPO_NAME%'; $launcher='%LAUNCHER_JAR%';" ^
  "$headers=@{ 'User-Agent'='Suwayomi-Launcher-Updater' };" ^
  "$localVer='';" ^
  "if(Test-Path -LiteralPath $launcher){" ^
  "  Add-Type -AssemblyName System.IO.Compression.FileSystem;" ^
  "  $zip=[System.IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $launcher));" ^
  "  try{ $entry=$zip.GetEntry('META-INF/MANIFEST.MF'); if($entry){ $r=[System.IO.StreamReader]::new($entry.Open()); try{$m=$r.ReadToEnd()} finally{$r.Dispose()} } } finally{ $zip.Dispose() }" ^
  "  if($m){ $mm=[regex]::Match($m,'(?m)^Implementation-Version:\s*(.+?)\s*$'); if($mm.Success){ $localVer=$mm.Groups[1].Value.Trim() } }" ^
  "}" ^
  "$release=Invoke-RestMethod -Uri ('https://api.github.com/repos/{0}/{1}/releases/latest' -f $owner,$repo) -Headers $headers;" ^
  "if($localVer -and $localVer -eq $release.tag_name -and (Test-Path -LiteralPath $launcher)){ exit 0 }" ^
  "$asset=$release.assets | Where-Object name -like '*.jar' | Select-Object -First 1;" ^
  "if(-not $asset){ throw 'No .jar asset found in latest release.' }" ^
  "if(Test-Path -LiteralPath $launcher){ try{ $fs=[System.IO.File]::Open($launcher,'Open','ReadWrite','None'); $fs.Dispose() } catch { throw 'Launcher JAR is locked. Close the launcher before updating.' } }" ^
  "$tmp=Join-Path $env:TEMP ('Suwayomi-Launcher-{0}.jar' -f [guid]::NewGuid().ToString('N'));" ^
  "Invoke-WebRequest -Uri $asset.browser_download_url -Headers $headers -OutFile $tmp -UseBasicParsing;" ^
  "if((Get-Item $tmp).Length -lt 10000){ Remove-Item -LiteralPath $tmp -Force; throw 'Downloaded file is suspiciously small. Aborting update.' }" ^
  "Move-Item -LiteralPath $tmp -Destination $launcher -Force;"

if errorlevel 1 exit /b 1

start "" "jre\bin\javaw.exe" --add-exports=java.desktop/sun.awt=ALL-UNNAMED -jar "%LAUNCHER_JAR%" %*
exit /b 0