// +build dynamic

package gorocksdb

// #cgo CXXFLAGS: -std=c++11
// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
import "C"
