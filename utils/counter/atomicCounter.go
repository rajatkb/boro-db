package counter

type Counter interface {
	Get() uint64
}

// atomic global counter (will have its own file with a name)
