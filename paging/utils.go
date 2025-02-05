package paging

func GetEmptyPage[S any, T TranslatePage[S]](key uint64, translatedPageHolder T) Page[S, T] {
	page := Page[S, T]{
		Key:      key,
		Dirty:    false,
		PageData: translatedPageHolder,
	}

	return page
}
