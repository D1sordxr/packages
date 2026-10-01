package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConfigConnectionString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "dsn wins",
			cfg:  Config{DSN: "postgres://u:p@h/db", Host: "ignored"},
			want: "postgres://u:p@h/db",
		},
		{
			name: "built from fields",
			cfg:  Config{Host: "pg", Port: 5432, Database: "db", User: "u", Password: "p"},
			want: "postgres://u:p@pg:5432/db",
		},
		{
			name: "with ssl mode",
			cfg:  Config{Host: "pg", Port: 5432, Database: "db", User: "u", Password: "p", SSLMode: "disable"},
			want: "postgres://u:p@pg:5432/db?sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.cfg.ConnectionString(); got != tt.want {
				t.Fatalf("ConnectionString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfigConnectionStringEscapesValues(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Host:     "pg",
		Port:     6432,
		Database: "my db",
		User:     "user@corp",
		Password: `p@ss w'rd/:?#\`,
		SSLMode:  "require",
	}

	parsed, err := pgxpool.ParseConfig(cfg.ConnectionString())
	if err != nil {
		t.Fatalf("ParseConfig(%q) = %v", cfg.ConnectionString(), err)
	}

	conn := parsed.ConnConfig
	if conn.Host != cfg.Host || conn.Port != 6432 || conn.Database != cfg.Database ||
		conn.User != cfg.User || conn.Password != cfg.Password {
		t.Fatalf("parsed = host %q port %d db %q user %q password %q, want %+v",
			conn.Host, conn.Port, conn.Database, conn.User, conn.Password, cfg)
	}
}
