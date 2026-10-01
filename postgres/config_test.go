package postgres

import "testing"

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
			want: "host=pg user=u password=p dbname=db port=5432",
		},
		{
			name: "with ssl mode",
			cfg:  Config{Host: "pg", Port: 5432, Database: "db", User: "u", Password: "p", SSLMode: "disable"},
			want: "host=pg user=u password=p dbname=db port=5432 sslmode=disable",
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
