# Copyright (c) Microsoft Corporation. Licensed under the MIT License.

$ErrorActionPreference = 'Stop'
[version]$minimumVersion = '2.2.0'

$mod = Import-Module Az.Accounts -MinimumVersion $minimumVersion -PassThru -ErrorAction SilentlyContinue

if (-not $mod) {
    Write-Error 'Az.Accounts module not found'
}

$params = @{
    ResourceUrl   = 'https://storage.azure.com/'
	WarningAction = 'Ignore'
}

# Only force AsSecureString for Az.Accounts versions > 2.17.0 and < 5.0.0 which return plain text token by default.
# Newer Az.Accounts versions return SecureString token by default and no longer use AsSecureString parameter.
if ($mod.Version -ge [version]'2.17.0' -and $mod.Version -lt [version]'5.0.0') {
    $params['AsSecureString'] = $true
}

$tenantId = ''
if ($tenantId.Length -gt 0) {
    $params['TenantId'] = ''
}

$token = Get-AzAccessToken @params

$customToken = New-Object -TypeName psobject

# The following .NET interop pattern is supported in all PowerShell versions and safely converts SecureString to plain text.
if ($token.Token -is [System.Security.SecureString]) {
    $ssPtr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($token.Token)
    try {
        $plainToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ssPtr)
    } finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ssPtr)
    }
    $customToken | Add-Member -MemberType NoteProperty -Name Token -Value $plainToken
} else {
    $customToken | Add-Member -MemberType NoteProperty -Name Token -Value $token.Token
}
$customToken | Add-Member -MemberType NoteProperty -Name ExpiresOn -Value $token.ExpiresOn.ToUnixTimeSeconds()

$jsonToken = $customToken | ConvertTo-Json
return $jsonToken
