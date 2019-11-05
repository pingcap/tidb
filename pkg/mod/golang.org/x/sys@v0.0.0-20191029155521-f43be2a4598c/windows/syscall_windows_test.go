// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package windows_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWin32finddata(t *testing.T) {
	dir, err := ioutil.TempDir("", "go-build")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "long_name.and_extension")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create %v: %v", path, err)
	}
	f.Close()

	type X struct {
		fd  windows.Win32finddata
		got byte
		pad [10]byte // to protect ourselves

	}
	var want byte = 2 // it is unlikely to have this character in the filename
	x := X{got: want}

	pathp, _ := windows.UTF16PtrFromString(path)
	h, err := windows.FindFirstFile(pathp, &(x.fd))
	if err != nil {
		t.Fatalf("FindFirstFile failed: %v", err)
	}
	err = windows.FindClose(h)
	if err != nil {
		t.Fatalf("FindClose failed: %v", err)
	}

	if x.got != want {
		t.Fatalf("memory corruption: want=%d got=%d", want, x.got)
	}
}

func TestFormatMessage(t *testing.T) {
	dll := windows.MustLoadDLL("netevent.dll")

	const TITLE_SC_MESSAGE_BOX uint32 = 0xC0001B75
	const flags uint32 = syscall.FORMAT_MESSAGE_FROM_HMODULE | syscall.FORMAT_MESSAGE_ARGUMENT_ARRAY | syscall.FORMAT_MESSAGE_IGNORE_INSERTS
	buf := make([]uint16, 300)
	_, err := windows.FormatMessage(flags, uintptr(dll.Handle), TITLE_SC_MESSAGE_BOX, 0, buf, nil)
	if err != nil {
		t.Fatalf("FormatMessage for handle=%x and errno=%x failed: %v", dll.Handle, TITLE_SC_MESSAGE_BOX, err)
	}
}

func abort(funcname string, err error) {
	panic(funcname + " failed: " + err.Error())
}

func ExampleLoadLibrary() {
	h, err := windows.LoadLibrary("kernel32.dll")
	if err != nil {
		abort("LoadLibrary", err)
	}
	defer windows.FreeLibrary(h)
	proc, err := windows.GetProcAddress(h, "GetVersion")
	if err != nil {
		abort("GetProcAddress", err)
	}
	r, _, _ := syscall.Syscall(uintptr(proc), 0, 0, 0, 0)
	major := byte(r)
	minor := uint8(r >> 8)
	build := uint16(r >> 16)
	print("windows version ", major, ".", minor, " (Build ", build, ")\n")
}

func TestTOKEN_ALL_ACCESS(t *testing.T) {
	if windows.TOKEN_ALL_ACCESS != 0xF01FF {
		t.Errorf("TOKEN_ALL_ACCESS = %x, want 0xF01FF", windows.TOKEN_ALL_ACCESS)
	}
}

func TestCreateWellKnownSid(t *testing.T) {
	sid, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		t.Fatalf("Unable to create well known sid for administrators: %v", err)
	}
	if got, want := sid.String(), "S-1-5-32-544"; got != want {
		t.Fatalf("Builtin Administrators SID = %s, want %s", got, want)
	}
}

func TestPseudoTokens(t *testing.T) {
	version, err := windows.GetVersion()
	if err != nil {
		t.Fatal(err)
	}
	if ((version&0xffff)>>8)|((version&0xff)<<8) < 0x0602 {
		return
	}

	realProcessToken, err := windows.OpenCurrentProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	defer realProcessToken.Close()
	realProcessUser, err := realProcessToken.GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}

	pseudoProcessToken := windows.GetCurrentProcessToken()
	pseudoProcessUser, err := pseudoProcessToken.GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	if !windows.EqualSid(realProcessUser.User.Sid, pseudoProcessUser.User.Sid) {
		t.Fatal("The real process token does not have the same as the pseudo process token")
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	err = windows.RevertToSelf()
	if err != nil {
		t.Fatal(err)
	}

	pseudoThreadToken := windows.GetCurrentThreadToken()
	_, err = pseudoThreadToken.GetTokenUser()
	if err != windows.ERROR_NO_TOKEN {
		t.Fatal("Expected an empty thread token")
	}
	pseudoThreadEffectiveToken := windows.GetCurrentThreadEffectiveToken()
	pseudoThreadEffectiveUser, err := pseudoThreadEffectiveToken.GetTokenUser()
	if err != nil {
		t.Fatal(nil)
	}
	if !windows.EqualSid(realProcessUser.User.Sid, pseudoThreadEffectiveUser.User.Sid) {
		t.Fatal("The real process token does not have the same as the pseudo thread effective token, even though we aren't impersonating")
	}

	err = windows.ImpersonateSelf(windows.SecurityImpersonation)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.RevertToSelf()
	pseudoThreadUser, err := pseudoThreadToken.GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	if !windows.EqualSid(realProcessUser.User.Sid, pseudoThreadUser.User.Sid) {
		t.Fatal("The real process token does not have the same as the pseudo thread token after impersonating self")
	}
}

func TestGUID(t *testing.T) {
	guid, err := windows.GenerateGUID()
	if err != nil {
		t.Fatal(err)
	}
	if guid.Data1 == 0 && guid.Data2 == 0 && guid.Data3 == 0 && guid.Data4 == [8]byte{} {
		t.Fatal("Got an all zero GUID, which is overwhelmingly unlikely")
	}
	want := fmt.Sprintf("{%08X-%04X-%04X-%04X-%012X}", guid.Data1, guid.Data2, guid.Data3, guid.Data4[:2], guid.Data4[2:])
	got := guid.String()
	if got != want {
		t.Fatalf("String = %q; want %q", got, want)
	}
	guid2, err := windows.GUIDFromString(got)
	if err != nil {
		t.Fatal(err)
	}
	if guid2 != guid {
		t.Fatalf("Did not parse string back to original GUID = %q; want %q", guid2, guid)
	}
	_, err = windows.GUIDFromString("not-a-real-guid")
	if err != syscall.Errno(windows.CO_E_CLASSSTRING) {
		t.Fatalf("Bad GUID string error = %v; want CO_E_CLASSSTRING", err)
	}
}

func TestKnownFolderPath(t *testing.T) {
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	defer token.Close()
	profileDir, err := token.GetUserProfileDirectory()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(profileDir, "Desktop")
	got, err := windows.KnownFolderPath(windows.FOLDERID_Desktop, windows.KF_FLAG_DEFAULT)
	if err != nil {
		t.Fatal(err)
	}
	if want != got {
		t.Fatalf("Path = %q; want %q", got, want)
	}
}

func TestRtlGetVersion(t *testing.T) {
	version := windows.RtlGetVersion()
	major, minor, build := windows.RtlGetNtVersionNumbers()
	// Go is not explictly added to the application compatibility database, so
	// these two functions should return the same thing.
	if version.MajorVersion != major || version.MinorVersion != minor || version.BuildNumber != build {
		t.Fatalf("%d.%d.%d != %d.%d.%d", version.MajorVersion, version.MinorVersion, version.BuildNumber, major, minor, build)
	}
}

func TestGetNamedSecurityInfo(t *testing.T) {
	path, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	if !sd.IsValid() {
		t.Fatal("Invalid security descriptor")
	}
	sdOwner, _, err := sd.Owner()
	if err != nil {
		t.Fatal(err)
	}
	if !sdOwner.IsValid() {
		t.Fatal("Invalid security descriptor owner")
	}
}

func TestGetSecurityInfo(t *testing.T) {
	sd, err := windows.GetSecurityInfo(windows.CurrentProcess(), windows.SE_KERNEL_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	if !sd.IsValid() {
		t.Fatal("Invalid security descriptor")
	}
	sdStr := sd.String()
	if !strings.HasPrefix(sdStr, "D:(A;") {
		t.Fatalf("DACL = %q; want D:(A;...", sdStr)
	}
}

func TestSddlConversion(t *testing.T) {
	sd, err := windows.SecurityDescriptorFromString("O:BA")
	if err != nil {
		t.Fatal(err)
	}
	if !sd.IsValid() {
		t.Fatal("Invalid security descriptor")
	}
	sdOwner, _, err := sd.Owner()
	if err != nil {
		t.Fatal(err)
	}
	if !sdOwner.IsValid() {
		t.Fatal("Invalid security descriptor owner")
	}
	if !sdOwner.IsWellKnown(windows.WinBuiltinAdministratorsSid) {
		t.Fatalf("Owner = %q; want S-1-5-32-544", sdOwner)
	}
}

func TestBuildSecurityDescriptor(t *testing.T) {
	const want = "O:SYD:(A;;GA;;;BA)"

	adminSid, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		t.Fatal(err)
	}
	systemSid, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		t.Fatal(err)
	}

	access := []windows.EXPLICIT_ACCESS{{
		AccessPermissions: windows.GENERIC_ALL,
		AccessMode:        windows.GRANT_ACCESS,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  windows.TRUSTEE_IS_GROUP,
			TrusteeValue: windows.TrusteeValueFromSID(adminSid),
		},
	}}
	owner := &windows.TRUSTEE{
		TrusteeForm:  windows.TRUSTEE_IS_SID,
		TrusteeType:  windows.TRUSTEE_IS_USER,
		TrusteeValue: windows.TrusteeValueFromSID(systemSid),
	}

	sd, err := windows.BuildSecurityDescriptor(owner, nil, access, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sd, err = sd.ToAbsolute()
	if err != nil {
		t.Fatal(err)
	}
	err = sd.SetSACL(nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := sd.String(); got != want {
		t.Fatalf("SD = %q; want %q", got, want)
	}
	sd, err = sd.ToSelfRelative()
	if err != nil {
		t.Fatal(err)
	}
	if got := sd.String(); got != want {
		t.Fatalf("SD = %q; want %q", got, want)
	}

	sd, err = windows.NewSecurityDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	acl, err := windows.ACLFromEntries(access, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = sd.SetDACL(acl, true, false)
	if err != nil {
		t.Fatal(err)
	}
	err = sd.SetOwner(systemSid, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := sd.String(); got != want {
		t.Fatalf("SD = %q; want %q", got, want)
	}
	sd, err = sd.ToSelfRelative()
	if err != nil {
		t.Fatal(err)
	}
	if got := sd.String(); got != want {
		t.Fatalf("SD = %q; want %q", got, want)
	}
}

func TestGetDiskFreeSpaceEx(t *testing.T) {
	cwd, err := windows.UTF16PtrFromString(".")
	if err != nil {
		t.Fatalf(`failed to call UTF16PtrFromString("."): %v`, err)
	}
	var freeBytesAvailableToCaller, totalNumberOfBytes, totalNumberOfFreeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(cwd, &freeBytesAvailableToCaller, &totalNumberOfBytes, &totalNumberOfFreeBytes); err != nil {
		t.Fatalf("failed to call GetDiskFreeSpaceEx: %v", err)
	}

	if freeBytesAvailableToCaller == 0 {
		t.Errorf("freeBytesAvailableToCaller: got 0; want > 0")
	}
	if totalNumberOfBytes == 0 {
		t.Errorf("totalNumberOfBytes: got 0; want > 0")
	}
	if totalNumberOfFreeBytes == 0 {
		t.Errorf("totalNumberOfFreeBytes: got 0; want > 0")
	}
}
