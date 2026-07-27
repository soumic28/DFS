// Package db embeds the SQL migrations so they ship inside the binary.
//
// Migrations run at coordinator startup rather than from a separate job or
// container: the schema then cannot drift from the code that expects it, and
// a rollback of the image is a rollback of the schema expectations with it.
package db

import "embed"

// Migrations holds the goose migration files.
//
//go:embed migrations/*.sql
var Migrations embed.FS
