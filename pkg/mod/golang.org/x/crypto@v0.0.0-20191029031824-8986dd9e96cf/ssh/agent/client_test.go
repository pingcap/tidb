// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package agent

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// startOpenSSHAgent executes ssh-agent, and returns an Agent interface to it.
func startOpenSSHAgent(t *testing.T) (client ExtendedAgent, socket string, cleanup func()) {
	if testing.Short() {
		// ssh-agent is not always available, and the key
		// types supported vary by platform.
		t.Skip("skipping test due to -short")
	}

	bin, err := exec.LookPath("ssh-agent")
	if err != nil {
		t.Skip("could not find ssh-agent")
	}

	cmd := exec.Command(bin, "-s")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("cmd.Output: %v", err)
	}

	/* Output looks like:

		   SSH_AUTH_SOCK=/tmp/ssh-P65gpcqArqvH/agent.15541; export SSH_AUTH_SOCK;
	           SSH_AGENT_PID=15542; export SSH_AGENT_PID;
	           echo Agent pid 15542;
	*/
	fields := bytes.Split(out, []byte(";"))
	line := bytes.SplitN(fields[0], []byte("="), 2)
	line[0] = bytes.TrimLeft(line[0], "\n")
	if string(line[0]) != "SSH_AUTH_SOCK" {
		t.Fatalf("could not find key SSH_AUTH_SOCK in %q", fields[0])
	}
	socket = string(line[1])

	line = bytes.SplitN(fields[2], []byte("="), 2)
	line[0] = bytes.TrimLeft(line[0], "\n")
	if string(line[0]) != "SSH_AGENT_PID" {
		t.Fatalf("could not find key SSH_AGENT_PID in %q", fields[2])
	}
	pidStr := line[1]
	pid, err := strconv.Atoi(string(pidStr))
	if err != nil {
		t.Fatalf("Atoi(%q): %v", pidStr, err)
	}

	conn, err := net.Dial("unix", string(socket))
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}

	ac := NewClient(conn)
	return ac, socket, func() {
		proc, _ := os.FindProcess(pid)
		if proc != nil {
			proc.Kill()
		}
		conn.Close()
		os.RemoveAll(filepath.Dir(socket))
	}
}

func startAgent(t *testing.T, agent Agent) (client ExtendedAgent, cleanup func()) {
	c1, c2, err := netPipe()
	if err != nil {
		t.Fatalf("netPipe: %v", err)
	}
	go ServeAgent(agent, c2)

	return NewClient(c1), func() {
		c1.Close()
		c2.Close()
	}
}

// startKeyringAgent uses Keyring to simulate a ssh-agent Server and returns a client.
func startKeyringAgent(t *testing.T) (client ExtendedAgent, cleanup func()) {
	return startAgent(t, NewKeyring())
}

func testOpenSSHAgent(t *testing.T, key interface{}, cert *ssh.Certificate, lifetimeSecs uint32) {
	agent, _, cleanup := startOpenSSHAgent(t)
	defer cleanup()

	testAgentInterface(t, agent, key, cert, lifetimeSecs)
}

func testKeyringAgent(t *testing.T, key interface{}, cert *ssh.Certificate, lifetimeSecs uint32) {
	agent, cleanup := startKeyringAgent(t)
	defer cleanup()

	testAgentInterface(t, agent, key, cert, lifetimeSecs)
}

func testAgentInterface(t *testing.T, agent ExtendedAgent, key interface{}, cert *ssh.Certificate, lifetimeSecs uint32) {
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("NewSignerFromKey(%T): %v", key, err)
	}
	// The agent should start up empty.
	if keys, err := agent.List(); err != nil {
		t.Fatalf("RequestIdentities: %v", err)
	} else if len(keys) > 0 {
		t.Fatalf("got %d keys, want 0: %v", len(keys), keys)
	}

	// Attempt to insert the key, with certificate if specified.
	var pubKey ssh.PublicKey
	if cert != nil {
		err = agent.Add(AddedKey{
			PrivateKey:   key,
			Certificate:  cert,
			Comment:      "comment",
			LifetimeSecs: lifetimeSecs,
		})
		pubKey = cert
	} else {
		err = agent.Add(AddedKey{PrivateKey: key, Comment: "comment", LifetimeSecs: lifetimeSecs})
		pubKey = signer.PublicKey()
	}
	if err != nil {
		t.Fatalf("insert(%T): %v", key, err)
	}

	// Did the key get inserted successfully?
	if keys, err := agent.List(); err != nil {
		t.Fatalf("List: %v", err)
	} else if len(keys) != 1 {
		t.Fatalf("got %v, want 1 key", keys)
	} else if keys[0].Comment != "comment" {
		t.Fatalf("key comment: got %v, want %v", keys[0].Comment, "comment")
	} else if !bytes.Equal(keys[0].Blob, pubKey.Marshal()) {
		t.Fatalf("key mismatch")
	}

	// Can the agent make a valid signature?
	data := []byte("hello")
	sig, err := agent.Sign(pubKey, data)
	if err != nil {
		t.Fatalf("Sign(%s): %v", pubKey.Type(), err)
	}

	if err := pubKey.Verify(data, sig); err != nil {
		t.Fatalf("Verify(%s): %v", pubKey.Type(), err)
	}

	// For tests on RSA keys, try signing with SHA-256 and SHA-512 flags
	if pubKey.Type() == "ssh-rsa" {
		sshFlagTest := func(flag SignatureFlags, expectedSigFormat string) {
			sig, err = agent.SignWithFlags(pubKey, data, flag)
			if err != nil {
				t.Fatalf("SignWithFlags(%s): %v", pubKey.Type(), err)
			}
			if sig.Format != expectedSigFormat {
				t.Fatalf("Signature format didn't match expected value: %s != %s", sig.Format, expectedSigFormat)
			}
			if err := pubKey.Verify(data, sig); err != nil {
				t.Fatalf("Verify(%s): %v", pubKey.Type(), err)
			}
		}
		sshFlagTest(0, ssh.SigAlgoRSA)
		sshFlagTest(SignatureFlagRsaSha256, ssh.SigAlgoRSASHA2256)
		sshFlagTest(SignatureFlagRsaSha512, ssh.SigAlgoRSASHA2512)
	}

	// If the key has a lifetime, is it removed when it should be?
	if lifetimeSecs > 0 {
		time.Sleep(time.Second*time.Duration(lifetimeSecs) + 100*time.Millisecond)
		keys, err := agent.List()
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(keys) > 0 {
			t.Fatalf("key not expired")
		}
	}

}

func TestMalformedRequests(t *testing.T) {
	keyringAgent := NewKeyring()
	listener, err := netListener()
	if err != nil {
		t.Fatalf("netListener: %v", err)
	}
	defer listener.Close()

	testCase := func(t *testing.T, requestBytes []byte, wantServerErr bool) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := listener.Accept()
			if err != nil {
				t.Errorf("listener.Accept: %v", err)
				return
			}
			defer c.Close()

			err = ServeAgent(keyringAgent, c)
			if err == nil {
				t.Error("ServeAgent should have returned an error to malformed input")
			} else {
				if (err != io.EOF) != wantServerErr {
					t.Errorf("ServeAgent returned expected error: %v", err)
				}
			}
		}()

		c, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("net.Dial: %v", err)
		}
		_, err = c.Write(requestBytes)
		if err != nil {
			t.Errorf("Unexpected error writing raw bytes on connection: %v", err)
		}
		c.Close()
		wg.Wait()
	}

	var testCases = []struct {
		name          string
		requestBytes  []byte
		wantServerErr bool
	}{
		{"Empty request", []byte{}, false},
		{"Short header", []byte{0x00}, true},
		{"Empty body", []byte{0x00, 0x00, 0x00, 0x00}, true},
		{"Short body", []byte{0x00, 0x00, 0x00, 0x01}, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { testCase(t, tc.requestBytes, tc.wantServerErr) })
	}
}

func TestAgent(t *testing.T) {
	for _, keyType := range []string{"rsa", "dsa", "ecdsa", "ed25519"} {
		testOpenSSHAgent(t, testPrivateKeys[keyType], nil, 0)
		testKeyringAgent(t, testPrivateKeys[keyType], nil, 0)
	}
}

func TestCert(t *testing.T) {
	cert := &ssh.Certificate{
		Key:         testPublicKeys["rsa"],
		ValidBefore: ssh.CertTimeInfinity,
		CertType:    ssh.UserCert,
	}
	cert.SignCert(rand.Reader, testSigners["ecdsa"])

	testOpenSSHAgent(t, testPrivateKeys["rsa"], cert, 0)
	testKeyringAgent(t, testPrivateKeys["rsa"], cert, 0)
}

// netListener creates a localhost network listener.
func netListener() (net.Listener, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		listener, err = net.Listen("tcp", "[::1]:0")
		if err != nil {
			return nil, err
		}
	}
	return listener, nil
}

// netPipe is analogous to net.Pipe, but it uses a real net.Conn, and
// therefore is buffered (net.Pipe deadlocks if both sides start with
// a write.)
func netPipe() (net.Conn, net.Conn, error) {
	listener, err := netListener()
	if err != nil {
		return nil, nil, err
	}
	defer listener.Close()
	c1, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		return nil, nil, err
	}

	c2, err := listener.Accept()
	if err != nil {
		c1.Close()
		return nil, nil, err
	}

	return c1, c2, nil
}

func TestServerResponseTooLarge(t *testing.T) {
	a, b, err := netPipe()
	if err != nil {
		t.Fatalf("netPipe: %v", err)
	}

	defer a.Close()
	defer b.Close()

	var response identitiesAnswerAgentMsg
	response.NumKeys = 1
	response.Keys = make([]byte, maxAgentResponseBytes+1)

	agent := NewClient(a)
	go func() {
		n, _ := b.Write(ssh.Marshal(response))
		if n < 4 {
			t.Fatalf("At least 4 bytes (the response size) should have been successfully written: %d < 4", n)
		}
	}()
	_, err = agent.List()
	if err == nil {
		t.Fatal("Did not get error result")
	}
	if err.Error() != "agent: client error: response too large" {
		t.Fatal("Did not get expected error result")
	}
}

func TestAuth(t *testing.T) {
	agent, _, cleanup := startOpenSSHAgent(t)
	defer cleanup()

	a, b, err := netPipe()
	if err != nil {
		t.Fatalf("netPipe: %v", err)
	}

	defer a.Close()
	defer b.Close()

	if err := agent.Add(AddedKey{PrivateKey: testPrivateKeys["rsa"], Comment: "comment"}); err != nil {
		t.Errorf("Add: %v", err)
	}

	serverConf := ssh.ServerConfig{}
	serverConf.AddHostKey(testSigners["rsa"])
	serverConf.PublicKeyCallback = func(c ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if bytes.Equal(key.Marshal(), testPublicKeys["rsa"].Marshal()) {
			return nil, nil
		}

		return nil, errors.New("pubkey rejected")
	}

	go func() {
		conn, _, _, err := ssh.NewServerConn(a, &serverConf)
		if err != nil {
			t.Fatalf("Server: %v", err)
		}
		conn.Close()
	}()

	conf := ssh.ClientConfig{
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	conf.Auth = append(conf.Auth, ssh.PublicKeysCallback(agent.Signers))
	conn, _, _, err := ssh.NewClientConn(b, "", &conf)
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	conn.Close()
}

func TestLockOpenSSHAgent(t *testing.T) {
	agent, _, cleanup := startOpenSSHAgent(t)
	defer cleanup()
	testLockAgent(agent, t)
}

func TestLockKeyringAgent(t *testing.T) {
	agent, cleanup := startKeyringAgent(t)
	defer cleanup()
	testLockAgent(agent, t)
}

func testLockAgent(agent Agent, t *testing.T) {
	if err := agent.Add(AddedKey{PrivateKey: testPrivateKeys["rsa"], Comment: "comment 1"}); err != nil {
		t.Errorf("Add: %v", err)
	}
	if err := agent.Add(AddedKey{PrivateKey: testPrivateKeys["dsa"], Comment: "comment dsa"}); err != nil {
		t.Errorf("Add: %v", err)
	}
	if keys, err := agent.List(); err != nil {
		t.Errorf("List: %v", err)
	} else if len(keys) != 2 {
		t.Errorf("Want 2 keys, got %v", keys)
	}

	passphrase := []byte("secret")
	if err := agent.Lock(passphrase); err != nil {
		t.Errorf("Lock: %v", err)
	}

	if keys, err := agent.List(); err != nil {
		t.Errorf("List: %v", err)
	} else if len(keys) != 0 {
		t.Errorf("Want 0 keys, got %v", keys)
	}

	signer, _ := ssh.NewSignerFromKey(testPrivateKeys["rsa"])
	if _, err := agent.Sign(signer.PublicKey(), []byte("hello")); err == nil {
		t.Fatalf("Sign did not fail")
	}

	if err := agent.Remove(signer.PublicKey()); err == nil {
		t.Fatalf("Remove did not fail")
	}

	if err := agent.RemoveAll(); err == nil {
		t.Fatalf("RemoveAll did not fail")
	}

	if err := agent.Unlock(nil); err == nil {
		t.Errorf("Unlock with wrong passphrase succeeded")
	}
	if err := agent.Unlock(passphrase); err != nil {
		t.Errorf("Unlock: %v", err)
	}

	if err := agent.Remove(signer.PublicKey()); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if keys, err := agent.List(); err != nil {
		t.Errorf("List: %v", err)
	} else if len(keys) != 1 {
		t.Errorf("Want 1 keys, got %v", keys)
	}
}

func testOpenSSHAgentLifetime(t *testing.T) {
	agent, _, cleanup := startOpenSSHAgent(t)
	defer cleanup()
	testAgentLifetime(t, agent)
}

func testKeyringAgentLifetime(t *testing.T) {
	agent, cleanup := startKeyringAgent(t)
	defer cleanup()
	testAgentLifetime(t, agent)
}

func testAgentLifetime(t *testing.T, agent Agent) {
	for _, keyType := range []string{"rsa", "dsa", "ecdsa"} {
		// Add private keys to the agent.
		err := agent.Add(AddedKey{
			PrivateKey:   testPrivateKeys[keyType],
			Comment:      "comment",
			LifetimeSecs: 1,
		})
		if err != nil {
			t.Fatalf("add: %v", err)
		}
		// Add certs to the agent.
		cert := &ssh.Certificate{
			Key:         testPublicKeys[keyType],
			ValidBefore: ssh.CertTimeInfinity,
			CertType:    ssh.UserCert,
		}
		cert.SignCert(rand.Reader, testSigners[keyType])
		err = agent.Add(AddedKey{
			PrivateKey:   testPrivateKeys[keyType],
			Certificate:  cert,
			Comment:      "comment",
			LifetimeSecs: 1,
		})
		if err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	time.Sleep(1100 * time.Millisecond)
	if keys, err := agent.List(); err != nil {
		t.Errorf("List: %v", err)
	} else if len(keys) != 0 {
		t.Errorf("Want 0 keys, got %v", len(keys))
	}
}

type keyringExtended struct {
	*keyring
}

func (r *keyringExtended) Extension(extensionType string, contents []byte) ([]byte, error) {
	if extensionType != "my-extension@example.com" {
		return []byte{agentExtensionFailure}, nil
	}
	return append([]byte{agentSuccess}, contents...), nil
}

func TestAgentExtensions(t *testing.T) {
	agent, _, cleanup := startOpenSSHAgent(t)
	defer cleanup()
	_, err := agent.Extension("my-extension@example.com", []byte{0x00, 0x01, 0x02})
	if err == nil {
		t.Fatal("should have gotten agent extension failure")
	}

	agent, cleanup = startAgent(t, &keyringExtended{})
	defer cleanup()
	result, err := agent.Extension("my-extension@example.com", []byte{0x00, 0x01, 0x02})
	if err != nil {
		t.Fatalf("agent extension failure: %v", err)
	}
	if len(result) != 4 || !bytes.Equal(result, []byte{agentSuccess, 0x00, 0x01, 0x02}) {
		t.Fatalf("agent extension result invalid: %v", result)
	}

	_, err = agent.Extension("bad-extension@example.com", []byte{0x00, 0x01, 0x02})
	if err == nil {
		t.Fatal("should have gotten agent extension failure")
	}
}
