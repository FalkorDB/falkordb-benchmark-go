package main

// Vars only for git sha and diff handling

var GitSHA1 = ""

// internal function to return value of GitSHA1 var, which is filled in link time
func toolGitSHA1() string {
	return GitSHA1
}
