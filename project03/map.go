package main

import (
	"strings"
)

func Map(input string) (ans map[string]int) {
	ans = make(map[string]int)
	ss := strings.Fields(input)
	for _, v := range ss {
		word := strings.ToLower(v)
		for len(word) > 0 && (word[0] < 'a' || word[0] > 'z') {
			word = word[1:]
		}
		for len(word) > 0 && (word[len(word)-1] < 'a' || word[len(word)-1] > 'z') {
			word = word[:len(word)-1]
		}
		if word == "" {
			continue
		}
		ans[word]++
	}
	return
}
