#!/usr/bin/env python3
"""Native authentication token matches Go master's TestCheckScramble fixture."""

from pathlib import Path
import runpy
import unittest


token = runpy.run_path(str(Path(__file__).with_name("mysql-prepared-client.py")))[
    "native_password_token"
]


class NativePasswordTest(unittest.TestCase):
    def test_go_master_scramble(self):
        # pkg/parser/auth/mysql_native_password_test.go, fdfadb96b2cf.
        salt = bytes([85, 92, 45, 22, 58, 79, 107, 6, 122, 125,
                      58, 80, 12, 90, 103, 32, 90, 10, 74, 82])
        expected = bytes([24, 180, 183, 225, 166, 6, 81, 102, 70, 248,
                          199, 143, 91, 204, 169, 9, 161, 171, 203, 33])
        self.assertEqual(token(b"abc", salt), expected)

    def test_empty_password(self):
        self.assertEqual(token(b"", bytes(range(20))), b"")


if __name__ == "__main__":
    unittest.main()
