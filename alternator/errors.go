package main

import "errors"

var (
	// ErrKeyNotFound occurs when a Get was made for an unexisting key
	ErrKeyNotFound = errors.New("key not found")
)
