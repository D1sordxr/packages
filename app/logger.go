package app

// Logger is the minimal logger App needs. *slog.Logger satisfies it as is.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}
