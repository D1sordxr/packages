package tx

import (
	"context"
)

func (m *ManagerImpl) IsTx(ctx context.Context) error {
	if _, err := m.GetTxExecutor(ctx); err != nil {
		return err
	}
	return nil
}
