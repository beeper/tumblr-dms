package connector

import (
	"context"
	"errors"
	"fmt"
	"io"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/ifixrobots/tumblr-dms/pkg/msgconv"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

var errMatrixImageTooLarge = errors.New("matrix image is too large")

type matrixMediaUploadError struct {
	err error
}

func (e *matrixMediaUploadError) Error() string {
	return "matrix media upload failed"
}

func (e *matrixMediaUploadError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func messageCanUseImageMedia(message tumblr.Message) bool {
	return msgconv.CanUseImageMedia(message)
}

func (tc *TumblrClient) convertTumblrMessageWithMedia(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, message tumblr.Message) (*bridgev2.ConvertedMessage, error) {
	if !messageCanUseImageMedia(message) {
		return tc.convertTumblrMessage(message), nil
	}
	postRefGIFCandidates := message.Post.GIFPreviewCandidates()
	isPostRefGIF := message.Type == tumblr.MessageTypePostRef && len(postRefGIFCandidates) > 0

	client, clientErr := tc.tumblrClient()
	if clientErr != nil {
		if isPostRefGIF {
			return tc.fallbackTumblrPostRefGIF(message, clientErr), nil
		}
		return nil, clientErr
	}
	if intent == nil {
		err := fmt.Errorf("matrix media uploader is not available")
		if isPostRefGIF {
			return tc.fallbackTumblrPostRefGIF(message, err), nil
		}
		return nil, err
	}
	if isPostRefGIF {
		part, permanentlyUnavailable, err := tc.convertTumblrImage(ctx, portal, intent, client, message, postRefGIFCandidates, "image/gif", 0)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !permanentlyUnavailable {
				return nil, err
			}
			return tc.fallbackTumblrPostRefGIF(message, err), nil
		}
		if part == nil {
			return tc.fallbackTumblrPostRefGIF(message, fmt.Errorf("tumblr GIF conversion returned no message part")), nil
		}
		if caption := msgconv.PostRefMediaCaption(message.Post); caption != "" {
			part.Content.Body = caption
		}
		return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{part}}, nil
	}
	parts := make([]*bridgev2.ConvertedMessagePart, 0, len(message.Images))
	for index, image := range message.Images {
		partID := tumblrImagePartID(index)
		part, permanentlyUnavailable, err := tc.convertTumblrImage(ctx, portal, intent, client, message, image.Candidates(), "", index)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !permanentlyUnavailable {
				return nil, err
			}
			if log := tc.log(); log != nil {
				log.Warn().
					Str("message_id_hash", logIdentifierHash(message.ID)).
					Str("message_type", logMessageType(message.Type)).
					Int("logical_image_index", index).
					Msg("Using a Tumblr media notice after all image candidates failed")
			}
			part = tumblrImageFailureNotice(message, index, len(message.Images))
		}
		part.ID = partID
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return tc.convertTumblrMessage(message), nil
	}
	return &bridgev2.ConvertedMessage{Parts: parts}, nil
}

func (tc *TumblrClient) fallbackTumblrPostRefGIF(message tumblr.Message, err error) *bridgev2.ConvertedMessage {
	if log := tc.log(); log != nil {
		log.Warn().
			Err(err).
			Str("message_id_hash", logIdentifierHash(message.ID)).
			Str("message_type", logMessageType(message.Type)).
			Msg("Using Tumblr post link because GIF preview was unavailable")
	}
	return tc.convertTumblrMessage(message)
}

func (tc *TumblrClient) convertTumblrImage(
	ctx context.Context,
	portal *bridgev2.Portal,
	intent bridgev2.MatrixAPI,
	client *tumblr.Client,
	message tumblr.Message,
	candidates []tumblr.ImageAsset,
	requiredMIMEType string,
	logicalIndex int,
) (*bridgev2.ConvertedMessagePart, bool, error) {
	if len(candidates) == 0 {
		return nil, true, fmt.Errorf("tumblr image has no downloadable candidates")
	}
	var roomID id.RoomID
	if portal != nil && portal.Portal != nil {
		roomID = portal.MXID
	}
	var firstTransientErr error
	var lastPermanentErr error
	for candidateIndex, image := range candidates {
		downloaded, err := client.DownloadImage(ctx, image.URL, tumblr.DefaultMaxDownloadBytes)
		if err != nil {
			if ctx.Err() != nil {
				return nil, false, ctx.Err()
			}
			candidateErr := fmt.Errorf("candidate %d download failed: %w", candidateIndex, err)
			if tumblr.IsPermanentMediaDownloadError(err) {
				lastPermanentErr = candidateErr
			} else if firstTransientErr == nil {
				firstTransientErr = candidateErr
			}
			continue
		}
		if requiredMIMEType != "" && downloaded.MIMEType != requiredMIMEType {
			lastPermanentErr = fmt.Errorf("candidate %d did not contain the required image format", candidateIndex)
			continue
		}
		fileName := tumblrImageFileName(downloaded.MIMEType, logicalIndex)
		mxc, file, err := intent.UploadMedia(ctx, roomID, downloaded.Data, fileName, downloaded.MIMEType)
		if err != nil {
			if ctx.Err() != nil {
				return nil, false, ctx.Err()
			}
			return nil, false, &matrixMediaUploadError{err: err}
		}
		info := &event.FileInfo{
			MimeType: downloaded.MIMEType,
			Size:     len(downloaded.Data),
			Width:    max(image.Width, 0),
			Height:   max(image.Height, 0),
		}
		if downloaded.MIMEType == "image/gif" {
			info.IsAnimated = true
			info.MauGIF = true
		}
		eventType := event.EventMessage
		msgType := event.MsgImage
		if message.Type == tumblr.MessageTypeSticker {
			eventType = event.EventSticker
			msgType = event.CapMsgSticker
		}
		return &bridgev2.ConvertedMessagePart{
			Type: eventType,
			Content: &event.MessageEventContent{
				MsgType:  msgType,
				Body:     fileName,
				FileName: fileName,
				URL:      mxc,
				File:     file,
				Info:     info,
			},
			DBMetadata: &MessageMetadata{Type: msgconv.MessageMetadataType(message.Type)},
		}, false, nil
	}
	if firstTransientErr != nil {
		return nil, false, firstTransientErr
	}
	return nil, true, lastPermanentErr
}

func tumblrImageFailureNotice(message tumblr.Message, index, total int) *bridgev2.ConvertedMessagePart {
	mediaType := "image"
	if message.Type == tumblr.MessageTypeSticker {
		mediaType = "sticker"
	}
	body := fmt.Sprintf("Could not load Tumblr %s", mediaType)
	if total > 1 {
		body = fmt.Sprintf("Could not load Tumblr %s %d of %d", mediaType, index+1, total)
	}
	return &bridgev2.ConvertedMessagePart{
		Type: event.EventMessage,
		Content: &event.MessageEventContent{
			MsgType: event.MsgNotice,
			Body:    body,
		},
		DBMetadata: &MessageMetadata{Type: msgconv.MessageMetadataType(message.Type)},
	}
}

func tumblrImagePartID(index int) networkid.PartID {
	if index == 0 {
		return ""
	}
	return networkid.PartID(fmt.Sprintf("image-%06d", index))
}

func tumblrImageFileName(mimeType string, index int) string {
	name := "tumblr-image"
	if index > 0 {
		name += fmt.Sprintf("-%06d", index)
	}
	return name + tumblr.CanonicalImageExtension(mimeType)
}

func (tc *TumblrClient) downloadMatrixImage(ctx context.Context, msg *bridgev2.MatrixMessage) (tumblr.ImageUpload, error) {
	if msg == nil || msg.Content == nil {
		return tumblr.ImageUpload{}, unsupportedMatrixMessageError(fmt.Errorf("image message content is missing"))
	}
	if msg.Content.URL == "" && (msg.Content.File == nil || msg.Content.File.URL == "") {
		return tumblr.ImageUpload{}, unsupportedMatrixMessageError(fmt.Errorf("image message is missing media"))
	}
	if msg.Portal == nil || msg.Portal.Bridge == nil || msg.Portal.Bridge.Bot == nil {
		return tumblr.ImageUpload{}, fmt.Errorf("matrix media downloader is not available")
	}
	if info := msg.Content.GetInfo(); info != nil && int64(info.Size) > tumblr.DefaultMaxUploadBytes {
		return tumblr.ImageUpload{}, fmt.Errorf("%w: Tumblr images must be 5 MiB or smaller", bridgev2.ErrMediaTooLarge)
	}

	requireRawGIF := msg.Content.GetCapMsgType() == event.CapMsgGIF
	data, err := downloadMatrixMediaBounded(ctx, msg.Portal.Bridge.Bot, msg.Content.URL, msg.Content.File, tumblr.DefaultMaxUploadBytes)
	if err != nil {
		if ctx.Err() != nil {
			return tumblr.ImageUpload{}, ctx.Err()
		}
		switch {
		case errors.Is(err, errMatrixImageTooLarge):
			return tumblr.ImageUpload{}, fmt.Errorf("%w: Tumblr images must be 5 MiB or smaller", bridgev2.ErrMediaTooLarge)
		default:
			return tumblr.ImageUpload{}, fmt.Errorf("%w: %v", bridgev2.ErrMediaDownloadFailed, err)
		}
	}
	mimeType, err := tumblr.SniffImageMIME(data)
	if err != nil {
		return tumblr.ImageUpload{}, fmt.Errorf("%w: Tumblr images must be JPEG, PNG, WebP, or GIF", bridgev2.ErrUnsupportedMediaType)
	}
	if requireRawGIF && mimeType != "image/gif" {
		return tumblr.ImageUpload{}, fmt.Errorf("%w: Tumblr GIFs must be raw GIF image files", bridgev2.ErrUnsupportedMediaType)
	}
	return tumblr.ImageUpload{
		Data:        data,
		FileName:    msg.Content.GetFileName(),
		ContentType: mimeType,
	}, nil
}

func downloadMatrixMediaBounded(
	ctx context.Context,
	intent bridgev2.MatrixAPI,
	uri id.ContentURIString,
	file *event.EncryptedFileInfo,
	maxBytes int64,
) ([]byte, error) {
	// MatrixAPI's generic download helpers do not accept a byte limit. Use the
	// same authenticated appservice intent while bounding the response stream.
	asIntent, ok := intent.(*matrix.ASIntent)
	if !ok || asIntent.Matrix == nil {
		return nil, fmt.Errorf("bounded matrix media downloader is not available")
	}
	if file != nil {
		uri = file.URL
		if err := file.PrepareForDecryption(); err != nil {
			return nil, err
		}
	}
	parsedURI, err := uri.Parse()
	if err != nil {
		return nil, err
	}
	resp, err := asIntent.Matrix.Download(ctx, parsedURI)
	if err != nil {
		return nil, fmt.Errorf("failed to send download request: %w", err)
	}
	defer resp.Body.Close()
	if resp.ContentLength > maxBytes {
		return nil, errMatrixImageTooLarge
	}

	reader := io.ReadCloser(resp.Body)
	if file != nil {
		reader = file.DecryptStream(resp.Body)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, errMatrixImageTooLarge
	}
	if err = reader.Close(); err != nil {
		return nil, fmt.Errorf("failed to close response body: %w", err)
	}
	return data, nil
}
