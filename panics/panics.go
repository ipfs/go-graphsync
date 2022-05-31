package panics

import (
	"fmt"
	"runtime/debug"
)

// CallBackFn is a function that will get called with information about the panic
type CallBackFn func(recoverObj interface{}, debugStackTrace string)

// PanicHandler is a function that can be called with the result of revover() within a deferred
// to recover from panics and pass them to a callback it returns an error if a recovery was needed
type PanicHandler func(interface{}) error

// MakeHandler makes a handler that recovers from panics and passes them to the given callback
func MakeHandler(cb CallBackFn) PanicHandler {
	return func(obj interface{}) error {
		if obj == nil {
			return nil
		}
		stack := string(debug.Stack())
		if cb != nil {
			cb(obj, stack)
		}
		return RecoveredPanicErr{
			PanicObj:        obj,
			DebugStackTrace: stack,
		}
	}
}

// RecoveredPanicErr captures panics that happen during execution of a single request or response
// The assumption is we want to make sure all of graphsync doesn't go down cause a single block load
// or selector execution fails
type RecoveredPanicErr struct {
	PanicObj        interface{}
	DebugStackTrace string
}

func (rpe RecoveredPanicErr) Error() string {
	return fmt.Sprintf("recovered from panic: %v, stack trace: %s", rpe.PanicObj, rpe.DebugStackTrace)
}
