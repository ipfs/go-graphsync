package panics

import "runtime/debug"

// CallBackFn is a function that will get called with information about the panic
type CallBackFn func(recoverObj interface{}, debugStackTrace string)

// Handler is a function that can be called with defer to recover fromp panics and pass them to a callback
type Handler func()

// MakeHandler makes a handler that recovers from panics and passes them to the given callback
func MakeHandler(cb CallBackFn) Handler {
	return func() {
		obj := recover()
		if obj == nil {
			return
		}
		cb(obj, string(debug.Stack()))
	}
}
