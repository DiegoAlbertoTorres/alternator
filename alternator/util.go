package main

const initSeed = 500

/* Utility functions for working with keys */

func getFirstReturn(f func(interface{}) (interface{}, interface{}), arg interface{}) interface{} {
	ret, _ := f(arg)
	return ret
}
