package testutil

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// AssertReceive verifies that a channel returns a value before the given context closes, and writes into
// into out, which should be a pointer to the value type
func AssertReceive(ctx context.Context, t testing.TB, channel interface{}, out interface{}, errorMessage string) {
	t.Helper()
	AssertReceiveFirst(t, channel, out, errorMessage, ctx.Done())
}

// AssertReceiveFirst verifies that a channel returns a value on the specified channel before the other channels,
// and writes the value into out, which should be a pointer to the value type
func AssertReceiveFirst(t testing.TB, channel interface{}, out interface{}, errorMessage string, incorrectChannels ...interface{}) {
	t.Helper()
	chanValue := reflect.ValueOf(channel)
	outValue := reflect.ValueOf(out)
	require.Equal(t, reflect.Chan, chanValue.Kind(), "incorrect argument: should pass channel to read from")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, chanValue.Type().ChanDir(), "incorrect argument: should pass a receiving channel")
	require.Equal(t, reflect.Ptr, outValue.Kind(), "incorrect argument: should pass a pointer for out value")
	require.True(t, chanValue.Type().Elem().AssignableTo(outValue.Elem().Type()), "incorrect argument: out value is incorrect type")
	var incorrectSelectCases []reflect.SelectCase
	for _, incorrectChannel := range incorrectChannels {
		incorrectChanValue := reflect.ValueOf(incorrectChannel)
		require.Equal(t, reflect.Chan, incorrectChanValue.Kind(), "incorrect argument: should pass channel to read from")
		require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, incorrectChanValue.Type().ChanDir(), "incorrect argument: should pass a receiving channel")
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
	require.Equal(t, 0, chosen, errorMessage)
	require.True(t, recvOk, errorMessage)
	outValue.Elem().Set(recv)
}

// AssertDoesReceive verifies that a channel returns some value before the given context closes
func AssertDoesReceive(ctx context.Context, t testing.TB, channel interface{}, errorMessage string) {
	t.Helper()
	AssertDoesReceiveFirst(t, channel, errorMessage, ctx.Done())
}

// AssertDoesReceiveFirst asserts that the given channel receives a value before any of the other channels specified
func AssertDoesReceiveFirst(t testing.TB, channel interface{}, errorMessage string, incorrectChannels ...interface{}) {
	t.Helper()
	chanValue := reflect.ValueOf(channel)
	require.Equal(t, reflect.Chan, chanValue.Kind(), "incorrect argument: should pass channel to read from")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, chanValue.Type().ChanDir(), "incorrect argument: should pass a receiving channel")
	var incorrectSelectCases []reflect.SelectCase
	for _, incorrectChannel := range incorrectChannels {
		incorrectChanValue := reflect.ValueOf(incorrectChannel)
		require.Equal(t, reflect.Chan, incorrectChanValue.Kind(), "incorrect argument: should pass channel to read from")
		require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, incorrectChanValue.Type().ChanDir(), "incorrect argument: should pass a receiving channel")
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
	require.Equal(t, 0, chosen, errorMessage)
}

// AssertChannelEmpty verifies that a channel has no value currently
func AssertChannelEmpty(t testing.TB, channel interface{}, errorMessage string) {
	t.Helper()
	chanValue := reflect.ValueOf(channel)
	require.Equal(t, reflect.Chan, chanValue.Kind(), "incorrect argument: should pass channel to read from")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.RecvDir}, chanValue.Type().ChanDir(), "incorrect argument: should pass a receiving channel")
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
func AssertSends(ctx context.Context, t testing.TB, channel interface{}, in interface{}, errorMessage string) {
	t.Helper()
	chanValue := reflect.ValueOf(channel)
	inValue := reflect.ValueOf(in)
	require.Equal(t, reflect.Chan, chanValue.Kind(), "incorrect argument: should pass channel to send to")
	require.Contains(t, []reflect.ChanDir{reflect.BothDir, reflect.SendDir}, chanValue.Type().ChanDir(), "incorrect argument: should pass a sending channel")
	require.True(t, inValue.Type().AssignableTo(chanValue.Type().Elem()), "incorrect argument: in value is incorrect type")
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
	require.Equal(t, 0, chosen, errorMessage)
}
