package tx

import (
	"context"
)

func (r *ManagerImpl) IsTx(ctx context.Context) error {
	if _, err := r.GetTxExecutor(ctx); err != nil {
		return err
	}
	return nil
}
