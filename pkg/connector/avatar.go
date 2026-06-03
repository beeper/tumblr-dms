package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

func (tc *TumblrClient) blogAvatar(blog tumblr.Blog) *bridgev2.Avatar {
	avatarURL := bestAvatarURL(blog.Avatar)
	if avatarURL == "" {
		return nil
	}
	return &bridgev2.Avatar{
		ID: avatarIDForURL(avatarURL),
		Get: func(ctx context.Context) ([]byte, error) {
			if tc == nil || tc.client == nil {
				return nil, fmt.Errorf("tumblr client is not available for avatar download")
			}
			return tc.client.Download(ctx, avatarURL, tumblr.DefaultMaxDownloadBytes)
		},
	}
}

func bestAvatarURL(assets []tumblr.ImageAsset) string {
	bestURL := ""
	bestArea := -1
	for _, asset := range assets {
		if !isDownloadableAvatarURL(asset.URL) {
			continue
		}
		area := asset.Width * asset.Height
		if area > bestArea {
			bestArea = area
			bestURL = asset.URL
		}
	}
	return bestURL
}

func isDownloadableAvatarURL(rawURL string) bool {
	return tumblr.IsDownloadURLAllowed(rawURL)
}

func avatarIDForURL(rawURL string) networkid.AvatarID {
	sum := sha256.Sum256([]byte(rawURL))
	return networkid.AvatarID("tumblr-avatar:" + hex.EncodeToString(sum[:]))
}
