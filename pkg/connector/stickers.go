package connector

import (
	"cmp"
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/tumblr-dms/pkg/tumblr"
)

const stickerPackURLPrefix = "tumblr://sticker-pack/"
const stickerCacheTime = 5 * time.Minute
const stickerErrorCacheTime = 30 * time.Second

var _ bridgev2.StickerImportingNetworkAPI = (*TumblrClient)(nil)

type cachedStickerPack struct {
	pack      *bridgev2.ImportedImagePack
	fetchedAt time.Time
}

func (tc *TumblrClient) getStickerPacks(ctx context.Context) ([]tumblr.StickerPack, error) {
	if err := tc.requireLoggedIn(); err != nil {
		return nil, err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	tc.stickerLock.Lock()
	defer tc.stickerLock.Unlock()
	cacheTime := stickerCacheTime
	if tc.stickerTrayErr != nil {
		cacheTime = stickerErrorCacheTime
	}
	if time.Since(tc.stickerTrayFetchedAt) < cacheTime {
		return tc.stickerTray, tc.stickerTrayErr
	}
	packs, err := client.GetStickerPacks(ctx)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	err = wrapTumblrRespError(tc.handleRemoteError(err))
	tc.stickerTray, tc.stickerTrayErr, tc.stickerTrayFetchedAt = packs, err, time.Now()
	return packs, err
}

func (tc *TumblrClient) stickerPackMetadata(ctx context.Context, client *tumblr.Client, pack *tumblr.StickerPack) event.ImagePackMetadata {
	return event.ImagePackMetadata{
		DisplayName: cmp.Or(strings.TrimSpace(pack.Title), strings.TrimSpace(pack.Description)),
		AvatarURL:   tc.stickerPackIcon(ctx, client, pack),
		Usage:       []event.ImagePackUsage{event.ImagePackUsageSticker},
		BridgedPack: &event.BridgedStickerPack{Network: "tumblrdms", URL: stickerPackURLPrefix + pack.ID},
	}
}

func (tc *TumblrClient) stickerPackIcon(ctx context.Context, client *tumblr.Client, pack *tumblr.StickerPack) id.ContentURIString {
	candidates := pack.Image.Candidates()
	if len(candidates) == 0 {
		return ""
	}
	parsed, err := url.Parse(candidates[0].URL)
	if err != nil {
		return ""
	}
	// Media hosts rotate, so the cache is keyed by the CDN path.
	key := database.Key(fmt.Sprintf("tumblr.sticker_pack_icon.%x", sha256.Sum256([]byte(parsed.Path))))
	kv := tc.userLogin.Bridge.DB.KV
	if mxc := kv.Get(ctx, key); mxc != "" {
		return id.ContentURIString(mxc)
	}
	mxc, _, err := tc.uploadSticker(ctx, client, candidates[0], pack.ID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("pack_id", pack.ID).Msg("Failed to upload Tumblr sticker pack icon")
		return ""
	}
	kv.Set(ctx, key, string(mxc))
	return mxc
}

func (tc *TumblrClient) uploadSticker(ctx context.Context, client *tumblr.Client, image tumblr.ImageAsset, name string) (id.ContentURIString, *event.FileInfo, error) {
	downloaded, err := client.DownloadImage(ctx, image.URL, tc.maxDownloadBytes())
	if err != nil {
		return "", nil, err
	}
	mxc, _, err := tc.userLogin.Bridge.Bot.UploadMedia(ctx, "", downloaded.Data, name+tumblr.CanonicalImageExtension(downloaded.MIMEType), downloaded.MIMEType)
	if err != nil {
		return "", nil, err
	}
	info := &event.FileInfo{
		MimeType: downloaded.MIMEType,
		Size:     len(downloaded.Data),
		Width:    image.Width,
		Height:   image.Height,
	}
	return mxc, info, nil
}

func (tc *TumblrClient) ListImagePacks(ctx context.Context) ([]*event.ImagePackMetadata, error) {
	if !tc.beginOwnedOperation() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
	packs, err := tc.getStickerPacks(ctx)
	if err != nil {
		return nil, err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	metadata := make([]*event.ImagePackMetadata, 0, len(packs))
	for i := range packs {
		if packs[i].ID == "" {
			continue
		}
		meta := tc.stickerPackMetadata(ctx, client, &packs[i])
		metadata = append(metadata, &meta)
	}
	return metadata, nil
}

func (tc *TumblrClient) DownloadImagePack(ctx context.Context, packURL string) (*bridgev2.ImportedImagePack, error) {
	if !tc.beginOwnedOperation() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
	packID, ok := strings.CutPrefix(packURL, stickerPackURLPrefix)
	if !ok || packID == "" {
		return nil, mautrix.MNotFound.WithMessage("Unsupported sticker pack")
	}
	packs, err := tc.getStickerPacks(ctx)
	if err != nil {
		return nil, err
	}
	for i := range packs {
		if packs[i].ID == packID {
			return tc.downloadStickerPack(ctx, &packs[i])
		}
	}
	return nil, mautrix.MNotFound.WithMessage("Sticker pack is not available on this account")
}

func (tc *TumblrClient) downloadStickerPack(ctx context.Context, pack *tumblr.StickerPack) (*bridgev2.ImportedImagePack, error) {
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	// Preview and import both download the pack, so a recent result is reused.
	tc.stickerPackLock.Lock()
	defer tc.stickerPackLock.Unlock()
	if cached, ok := tc.stickerPacks[pack.ID]; ok && time.Since(cached.fetchedAt) < stickerCacheTime {
		return cached.pack, nil
	}
	metadata := tc.stickerPackMetadata(ctx, client, pack)
	imported := &bridgev2.ImportedImagePack{
		Shortcode: "tumblr_" + pack.ID,
		Content:   &event.ImagePackEventContent{Metadata: metadata, Images: make(map[string]*event.ImagePackImage)},
	}
	for _, sticker := range pack.Stickers {
		candidates := sticker.Image.Candidates()
		if sticker.ID == "" || len(candidates) == 0 {
			continue
		}
		mxc, info, err := tc.uploadSticker(ctx, client, candidates[0], sticker.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to import sticker %s: %w", sticker.ID, err)
		}
		if imported.Content.Metadata.AvatarURL == "" {
			imported.Content.Metadata.AvatarURL = mxc
		}
		info.BridgedSticker = &event.BridgedSticker{Network: "tumblrdms", ID: sticker.ID, PackURL: metadata.BridgedPack.URL}
		imported.Content.Images[sticker.ID] = &event.ImagePackImage{URL: mxc, Body: sticker.Description, Info: info}
	}
	if len(imported.Content.Images) == 0 {
		return nil, mautrix.MNotFound.WithMessage("Sticker pack is empty")
	}
	if tc.stickerPacks == nil {
		tc.stickerPacks = make(map[string]cachedStickerPack)
	}
	tc.stickerPacks[pack.ID] = cachedStickerPack{pack: imported, fetchedAt: time.Now()}
	return imported, nil
}
