#ifndef SourceRoot
  #define SourceRoot ".."
#endif

#define AppName "FormSight Local"
#define AppVersion "0.5.1"
#define AppPublisher "FormSight"
#define AppExeName "FormSightLocal.exe"

[Setup]
AppId={{8EA33A81-C68D-40D8-9C3A-E8D77BA26374}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={autopf}\FormSight Local
DefaultGroupName=FormSight Local
DisableProgramGroupPage=yes
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
PrivilegesRequired=admin
OutputDir={#SourceRoot}\release
OutputBaseFilename=FormSight-Local-Setup
Compression=lzma2/normal
SolidCompression=yes
WizardStyle=modern
UninstallDisplayIcon={app}\{#AppExeName}
CloseApplications=yes
RestartApplications=no
SetupMutex=FormSightLocalSetupMutex
AppMutex=FormSightLocalAppMutex

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut / 建立桌面捷徑"; GroupDescription: "Shortcuts / 捷徑:"; Flags: checkedonce

[Files]
Source: "{#SourceRoot}\release\app\FormSightLocal\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\FormSight Local"; Filename: "{app}\{#AppExeName}"
Name: "{autodesktop}\FormSight Local"; Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Launch FormSight Local / 啟動 FormSight 本機版"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}"

[Code]
function InitializeSetup(): Boolean;
begin
  Result := IsWin64;
  if not Result then
    MsgBox('FormSight Local requires 64-bit Windows 10 or 11.', mbError, MB_OK);
end;
