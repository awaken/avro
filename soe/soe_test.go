package soe_test

import (
	"testing"

	"github.com/awaken/avro/v2/soe"
	"github.com/stretchr/testify/require"
)

func TestProtocolMagicIsImmutable(t *testing.T) {
	original := append([]byte(nil), soe.Magic...)
	t.Cleanup(func() {
		soe.Magic = original
	})

	legacyMagic := []byte{0, 0}
	soe.Magic = legacyMagic

	fingerprint := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	header, err := soe.BuildHeaderForFingerprint(fingerprint)
	require.NoError(t, err)
	require.Equal(t, []byte{0xc3, 0x01}, header[:2])

	legacyMagic[0] = 0xff
	parsed, payload, err := soe.ParseHeader(header)
	require.NoError(t, err)
	require.Equal(t, fingerprint, parsed)
	require.Empty(t, payload)

	magic := soe.MagicBytes()
	magic[0] = 0
	require.Equal(t, []byte{0xc3, 0x01}, soe.MagicBytes())
}

func TestProtocolDoesNotReadMutableMagic(t *testing.T) {
	original := append([]byte(nil), soe.Magic...)
	legacyMagic := []byte{0xc3, 0x01}
	soe.Magic = legacyMagic
	t.Cleanup(func() {
		soe.Magic = original
	})

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10_000; i++ {
			legacyMagic[0]++
		}
		close(done)
	}()

	fingerprint := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	for i := 0; i < 10_000; i++ {
		header, err := soe.BuildHeaderForFingerprint(fingerprint)
		require.NoError(t, err)
		_, _, err = soe.ParseHeader(header)
		require.NoError(t, err)
	}
	<-done
}
