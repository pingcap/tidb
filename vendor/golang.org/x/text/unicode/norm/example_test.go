// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package norm_test

import (
	"fmt"

	"golang.org/x/text/unicode/norm"
)

func ExampleForm_NextBoundary() {
	s := norm.NFD.String("Mêlée")

	for i := 0; i < len(s); {
		d := norm.NFC.NextBoundaryInString(s[i:], true)
		fmt.Printf("%[1]s: %+[1]q\n", s[i:i+d])
		i += d
	}
	// Output:
	// M: "M"
	// ê: "e\u0302"
	// l: "l"
	// é: "e\u0301"
	// e: "e"
}
