package connector

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	"github.com/beeper/tumblr-dms/pkg/tumblr"
)

const stickerPackURLPrefix = "tumblr://sticker-pack/"
const stickerCacheTime = 5 * time.Minute
const stickerErrorCacheTime = 30 * time.Second

var _ bridgev2.StickerImportingNetworkAPI = (*TumblrClient)(nil)

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

func (tc *TumblrClient) stickerPackMetadata(ctx context.Context, pack *tumblr.StickerPack) event.ImagePackMetadata {
	metadata := event.ImagePackMetadata{
		DisplayName: cmp.Or(strings.TrimSpace(pack.Title), strings.TrimSpace(pack.Description)),
		Usage:       []event.ImagePackUsage{event.ImagePackUsageSticker},
		BridgedPack: &event.BridgedStickerPack{Network: "tumblrdms", URL: stickerPackURLPrefix + pack.ID},
	}
	if candidates := pack.Image.Candidates(); len(candidates) > 0 {
		if cached := tc.cachedSticker(ctx, candidates[0]); cached != nil {
			metadata.AvatarURL = cached.URL
		}
	}
	return metadata
}

func stickerCacheKey(image tumblr.ImageAsset) database.Key {
	return database.Key("stickercache:" + image.URL)
}

var errStickerUpload = errors.New("failed to upload sticker")

func (tc *TumblrClient) cachedSticker(ctx context.Context, image tumblr.ImageAsset) *event.ImagePackImage {
	var cached event.ImagePackImage
	if json.Unmarshal([]byte(tc.userLogin.Bridge.DB.KV.Get(ctx, stickerCacheKey(image))), &cached) != nil || cached.URL == "" || cached.Info == nil {
		return nil
	}
	return &cached
}

func (tc *TumblrClient) uploadSticker(ctx context.Context, client *tumblr.Client, image tumblr.ImageAsset, name string) (*event.ImagePackImage, error) {
	if cached := tc.cachedSticker(ctx, image); cached != nil {
		return cached, nil
	}
	downloaded, err := client.DownloadImage(ctx, image.URL, tc.maxDownloadBytes())
	if err != nil {
		return nil, err
	}
	mxc, _, err := tc.userLogin.Bridge.Bot.UploadMedia(ctx, "", downloaded.Data, name+tumblr.CanonicalImageExtension(downloaded.MIMEType), downloaded.MIMEType)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errStickerUpload, err)
	}
	uploaded := &event.ImagePackImage{URL: mxc, Info: &event.FileInfo{MimeType: downloaded.MIMEType, Size: len(downloaded.Data)}}
	if data, err := json.Marshal(uploaded); err == nil {
		tc.userLogin.Bridge.DB.KV.Set(ctx, stickerCacheKey(image), string(data))
	}
	return uploaded, nil
}

func (tc *TumblrClient) tumblrStickerImage(ctx context.Context, stickerID string) (tumblr.ImageUpload, bool) {
	if stickerID == "" {
		return tumblr.ImageUpload{}, false
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return tumblr.ImageUpload{}, false
	}
	packs, err := tc.getStickerPacks(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("sticker_id", stickerID).Msg("Failed to get Tumblr sticker packs, sending sticker as an image")
		return tumblr.ImageUpload{}, false
	}
	for _, pack := range packs {
		for _, sticker := range pack.Stickers {
			if candidates := sticker.Image.Candidates(); sticker.ID == stickerID && len(candidates) > 0 {
				downloaded, err := client.DownloadImage(ctx, candidates[0].URL, tc.maxDownloadBytes())
				if err != nil {
					zerolog.Ctx(ctx).Warn().Err(err).Str("sticker_id", stickerID).Msg("Failed to download Tumblr sticker, sending it as an image")
					return tumblr.ImageUpload{}, false
				}
				return tumblr.ImageUpload{Data: downloaded.Data, FileName: stickerID + tumblr.CanonicalImageExtension(downloaded.MIMEType), ContentType: downloaded.MIMEType}, true
			}
		}
	}
	return tumblr.ImageUpload{}, false
}

func tumblrStickerID(content *event.MessageEventContent) string {
	if content.MsgType != event.CapMsgSticker || content.Info == nil || content.Info.BridgedSticker == nil || content.Info.BridgedSticker.Network != "tumblrdms" {
		return ""
	}
	return content.Info.BridgedSticker.ID
}

func (tc *TumblrClient) ListImagePacks(ctx context.Context) ([]*event.ImagePackMetadata, error) {
	if !tc.beginOwnedOperation() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
	ctx, cancel, err := tc.generationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	packs, err := tc.getStickerPacks(ctx)
	if err != nil {
		return nil, err
	}
	metadata := make([]*event.ImagePackMetadata, 0, len(packs))
	for i := range packs {
		if packs[i].ID == "" {
			continue
		}
		meta := tc.stickerPackMetadata(ctx, &packs[i])
		metadata = append(metadata, &meta)
	}
	return metadata, nil
}

func (tc *TumblrClient) DownloadImagePack(ctx context.Context, packURL string) (*bridgev2.ImportedImagePack, error) {
	if !tc.beginOwnedOperation() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
	ctx, cancel, err := tc.generationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
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
	metadata := tc.stickerPackMetadata(ctx, pack)
	if candidates := pack.Image.Candidates(); metadata.AvatarURL == "" && len(candidates) > 0 {
		if icon, err := tc.uploadSticker(ctx, client, candidates[0], pack.ID); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("pack_id", pack.ID).Msg("Failed to upload Tumblr sticker pack icon")
		} else {
			metadata.AvatarURL = icon.URL
		}
	}
	imported := &bridgev2.ImportedImagePack{
		Shortcode: "tumblr_" + pack.ID,
		Content:   &event.ImagePackEventContent{Images: make(map[string]*event.ImagePackImage)},
	}
	for _, sticker := range pack.Stickers {
		candidates := sticker.Image.Candidates()
		if sticker.ID == "" || len(candidates) == 0 {
			continue
		}
		uploaded, err := tc.uploadSticker(ctx, client, candidates[0], sticker.ID)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		} else if errors.Is(err, errStickerUpload) || tumblr.IsTransientMediaDownloadError(err) {
			return nil, fmt.Errorf("failed to import sticker %s: %w", sticker.ID, err)
		} else if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("sticker_id", sticker.ID).Msg("Failed to download Tumblr sticker, skipping")
			continue
		}
		if metadata.AvatarURL == "" {
			metadata.AvatarURL = uploaded.URL
		}
		imported.Content.Images[sticker.ID] = &event.ImagePackImage{URL: uploaded.URL, Body: cmp.Or(strings.TrimSpace(sticker.Description), metadata.DisplayName), Info: &event.FileInfo{
			MimeType:       uploaded.Info.MimeType,
			Size:           uploaded.Info.Size,
			Width:          candidates[0].Width,
			Height:         candidates[0].Height,
			BridgedSticker: &event.BridgedSticker{Network: "tumblrdms", ID: sticker.ID, PackURL: metadata.BridgedPack.URL},
		}}
	}
	if len(imported.Content.Images) == 0 {
		return nil, mautrix.MNotFound.WithMessage("Sticker pack is empty")
	}
	imported.Content.Metadata = metadata
	return imported, nil
}
