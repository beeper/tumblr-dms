package connector

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
)

var _ bridgev2.RedactionHandlingNetworkAPI = (*TumblrClient)(nil)

func (tc *TumblrClient) HandleMatrixMessageRemove(context.Context, *bridgev2.MatrixMessageRemove) error {
	return bridgev2.ErrRedactionsNotSupported
}
