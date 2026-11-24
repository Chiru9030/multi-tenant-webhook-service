package web

import (
	"embed"
	"io/fs"
)

//go:embed static/*
var staticFiles embed.FS

// GetStaticFS returns an fs.FS rooted at the "static" directory
func GetStaticFS() (fs.FS, error) {
	return fs.Sub(staticFiles, "static")
}
