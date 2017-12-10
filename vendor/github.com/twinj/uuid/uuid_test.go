package uuid_test

import (
	"fmt"
	"github.com/twinj/uuid"
	"testing"
	"time"
)

const (
	print = "version %d variant %x: %s\n"
)

func Test_AllVersions(t *testing.T) {
	Test_NewV1(nil)
	Test_NewV3(nil)
	Test_NewV4(nil)
	Test_NewV5(nil)
}

func Test_NewV1(t *testing.T) {
	u1 := uuid.NewV1()
	fmt.Printf(print, u1.Version(), u1.Variant(), u1)
}

func Test_NewV3(t *testing.T) {
	u, _ := uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	u3 := uuid.NewV3(u, uuid.Name("test"))
	fmt.Printf(print, u3.Version(), u3.Variant(), u3)
}

func Test_NewV4(t *testing.T) {
	u4 := uuid.NewV4()
	fmt.Printf(print, u4.Version(), u4.Variant(), u4)
}

func Test_NewV5(t *testing.T) {
	u5 := uuid.NewV5(uuid.NamespaceURL, uuid.Name("test"))
	fmt.Printf(print, u5.Version(), u5.Variant(), u5)
}

func Test_Parse(t *testing.T) {
	u, err := uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(u)
}

func Example() {
	var config = uuid.StateSaverConfig{SaveReport: true, SaveSchedule: 30 * time.Minute}
	uuid.SetupFileSystemStateSaver(config)
	u1 := uuid.NewV1()
	fmt.Printf("version %d variant %x: %s\n", u1.Version(), u1.Variant(), u1)

	uP, _ := uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	u3 := uuid.NewV3(uP, uuid.Name("test"))

	u4 := uuid.NewV4()
	fmt.Printf("version %d variant %x: %s\n", u4.Version(), u4.Variant(), u4)

	u5 := uuid.NewV5(uuid.NamespaceURL, uuid.Name("test"))

	if uuid.Equal(u1, u3) {
		fmt.Printf("Will never happen")
	}

	fmt.Printf(uuid.Formatter(u5, uuid.CurlyHyphen))

	uuid.SwitchFormat(uuid.BracketHyphen)
}

func ExampleNewV1() {
	u1 := uuid.NewV1()
	fmt.Printf(print, u1.Version(), u1.Variant(), u1)
}

func ExampleNewV3() {
	u, _ := uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	u3 := uuid.NewV3(u, uuid.Name("test"))
	fmt.Printf("version %d variant %x: %s\n", u3.Version(), u3.Variant(), u3)
}

func ExampleNewV4() {
	u4 := uuid.NewV4()
	fmt.Printf("version %d variant %x: %s\n", u4.Version(), u4.Variant(), u4)
}

func ExampleNewV5() {
	u5 := uuid.NewV5(uuid.NamespaceURL, uuid.Name("test"))
	fmt.Printf("version %d variant %x: %s\n", u5.Version(), u5.Variant(), u5)
}

func ExampleParse() {
	u, err := uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(u)
}

func ExampleSetupFileSystemStateSaver() {
	var config = uuid.StateSaverConfig{SaveReport: true, SaveSchedule: 30 * time.Minute}
	uuid.SetupFileSystemStateSaver(config)
	u1 := uuid.NewV1()
	fmt.Printf("version %d variant %x: %s\n", u1.Version(), u1.Variant(), u1)
}

func ExampleFormatter() {
	u4 := uuid.NewV4()
	fmt.Printf(uuid.Formatter(u4, uuid.CurlyHyphen))
}

func ExampleSwitchFormat() {
	uuid.SwitchFormat(uuid.BracketHyphen)
	u4 := uuid.NewV4()
	fmt.Printf("version %d variant %x: %s\n", u4.Version(), u4.Variant(), u4)
}
