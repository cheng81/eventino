package common

import (
	"fmt"
	"os"
)

func Envset(k string) bool {
	_, ok := os.LookupEnv(k)
	fmt.Println("envset? ", k, ok)
	return ok
}

func Getenv(k, def string) string {
	var out string
	var ok bool
	if out, ok = os.LookupEnv(k); !ok {
		fmt.Println("getenv.cannot find key", k, "return default", def)
		return def
	}
	fmt.Println("getenv.found key", k, "return", out)
	return out
}
