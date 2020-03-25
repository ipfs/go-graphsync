package testutil

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// AssertReceive verifies that a channel returns a value before the given context closes, and writes into
// into out, which should be a pointer to the value type
func AssertReceive(ctx context.Context, t *testing.T, channel interface{}, out interface{}, errorMessage string) {
	AssertReceiveFirst(t, channel, out, errorMessage, ctx.Done())
}

// AssertReceiveFirst verifies that a channel returns a value on the specified channel before the other channels,
// and writes the value into out, which should be a pointer to the value type
func AssertReceiveFirst(t *testing.T, channel interface{}, out interface{}, errorMessage string, incorrectChannels ...interface{}) {
	chanValue := reflect.ValueOf(channel)
	outValue := reflect.ValueOf(out)
	require.Equal(t, chanValue.Kind(), reflect.Chan, "passes a channel to read from")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, chanValue.Type().ChanDir(), "passes a receiving channel")
	require.Equal(t, outValue.Kind(), reflect.Ptr, "passes a pointer for out value")
	require.True(t, chanValue.Type().Elem().AssignableTo(outValue.Elem().Type()), "out value is correct type")
	var incorrectSelectCases []reflect.SelectCase
	for _, incorrectChannel := range incorrectChannels {
		incorrectChanValue := reflect.ValueOf(incorrectChannel)
		require.Equal(t, incorrectChanValue.Kind(), reflect.Chan, "passes a channel to read from")
		require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, incorrectChanValue.Type().ChanDir(), "passes a receiving channel")
		incorrectSelectCases = append(incorrectSelectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: incorrectChanValue,
		})
	}
	chosen, recv, recvOk := reflect.Select(append([]reflect.SelectCase{
		{
			Dir:  reflect.SelectRecv,
			Chan: chanValue,
		},
	}, incorrectSelectCases...))
	require.Equal(t, chosen, 0, errorMessage)
	require.True(t, recvOk, errorMessage)
	outValue.Elem().Set(recv)
}

// AssertDoesReceive verifies that a channel returns some value before the given context closes
func AssertDoesReceive(ctx context.Context, t *testing.T, channel interface{}, errorMessage string) {
	AssertDoesReceiveFirst(t, channel, errorMessage, ctx.Done())
}

// AssertDoesReceiveFirst asserts that the given channel receives a value before any of the other channels specified
func AssertDoesReceiveFirst(t *testing.T, channel interface{}, errorMessage string, incorrectChannels ...interface{}) {
	chanValue := reflect.ValueOf(channel)
	require.Equal(t, chanValue.Kind(), reflect.Chan, "passes a channel to read from")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, chanValue.Type().ChanDir(), "passes a receiving channel")
	var incorrectSelectCases []reflect.SelectCase
	for _, incorrectChannel := range incorrectChannels {
		incorrectChanValue := reflect.ValueOf(incorrectChannel)
		require.Equal(t, incorrectChanValue.Kind(), reflect.Chan, "passes channel to read from")
		require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, incorrectChanValue.Type().ChanDir(), "passes a receiving channel")
		incorrectSelectCases = append(incorrectSelectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: incorrectChanValue,
		})
	}
	chosen, _, _ := reflect.Select(append([]reflect.SelectCase{
		{
			Dir:  reflect.SelectRecv,
			Chan: chanValue,
		},
	}, incorrectSelectCases...))
	require.Equal(t, chosen, 0, errorMessage)
}

// AssertChannelEmpty verifies that a channel has no value currently
func AssertChannelEmpty(t *testing.T, channel interface{}, errorMessage string) {
	chanValue := reflect.ValueOf(channel)
	require.Equal(t, chanValue.Kind(), reflect.Chan, "did not pass channel to read from")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, chanValue.Type().ChanDir(), "did not pass a receiving channel")
	chosen, _, _ := reflect.Select([]reflect.SelectCase{
		{
			Dir:  reflect.SelectRecv,
			Chan: chanValue,
		},
		{
			Dir: reflect.SelectDefault,
		},
	})
	require.NotEqual(t, chosen, 0, errorMessage)
}

// AssertSends attempts to send the given input value to the given channel before the given context closes
func AssertSends(ctx context.Context, t *testing.T, channel interface{}, in interface{}, errorMessage string) {
	chanValue := reflect.ValueOf(channel)
	inValue := reflect.ValueOf(in)
	require.Equal(t, chanValue.Kind(), reflect.Chan, "passes channel to send to")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.SendDir}, chanValue.Type().ChanDir(), "passes a sending channel")
	require.True(t, inValue.Type().AssignableTo(chanValue.Type().Elem()), "in value is correct type")
	chosen, _, _ := reflect.Select([]reflect.SelectCase{
		{
			Dir:  reflect.SelectSend,
			Chan: chanValue,
			Send: inValue,
		},
		{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		},
	})
	require.Equal(t, chosen, 0, errorMessage)
}
