package connector

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/mediaproxy"

	"github.com/beeper/tumblr-dms/pkg/tumblr"
)

var _ bridgev2.DirectMediableNetwork = (*TumblrConnector)(nil)

const directMediaHost = "media.tumblr.com"

func (tc *TumblrConnector) SetUseDirectMedia() {
	tc.directMedia = true
}

// Only the subdomain and path are stored to keep content URIs short.
func encodeDirectMediaID(rawURL string) networkid.MediaID {
	u, err := url.Parse(rawURL)
	if err != nil || !tumblr.IsDownloadURLAllowed(rawURL) || u.RawQuery != "" || u.Path == "" {
		return nil
	}
	subdomain, ok := strings.CutSuffix(u.Hostname(), directMediaHost)
	if !ok || !isDirectMediaSubdomain(subdomain) {
		return nil
	}
	return append([]byte{1}, subdomain+u.EscapedPath()...)
}

func parseDirectMediaID(mediaID networkid.MediaID) (string, error) {
	if len(mediaID) < 2 || mediaID[0] != 1 {
		return "", mediaproxy.ErrInvalidMediaIDSyntax
	}
	subdomain, path, _ := strings.Cut(string(mediaID[1:]), "/")
	if !isDirectMediaSubdomain(subdomain) {
		return "", mediaproxy.ErrInvalidMediaIDSyntax
	}
	return "https://" + subdomain + directMediaHost + "/" + path, nil
}

// Tumblr serves images from media.tumblr.com and single-label shards like 64.media.tumblr.com.
func isDirectMediaSubdomain(subdomain string) bool {
	label, ok := strings.CutSuffix(subdomain, ".")
	return subdomain == "" || ok && label != "" && strings.Trim(label, "abcdefghijklmnopqrstuvwxyz0123456789-") == ""
}

func (tc *TumblrClient) directMediaContent(
	ctx context.Context,
	portal *bridgev2.Portal,
	client *tumblr.Client,
	rawURL, requiredMIMEType string,
) (content *event.MessageEventContent, mismatched bool) {
	if tc.connector == nil || !tc.connector.directMedia {
		return nil, false
	}
	mediaID := encodeDirectMediaID(rawURL)
	if mediaID == nil {
		return nil, false
	}
	mimeType, size, err := client.HeadImage(ctx, rawURL)
	if err != nil || size > tc.maxDownloadBytes() {
		return nil, false
	} else if requiredMIMEType != "" && mimeType != requiredMIMEType {
		return nil, true
	}
	content = &event.MessageEventContent{Info: &event.FileInfo{MimeType: mimeType, Size: int(max(size, 0))}}
	content.URL, content.File, err = portal.Bridge.GenerateContentURI(ctx, portal.MXID, mediaID)
	if err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to generate direct media URI, reuploading Tumblr image")
		}
		return nil, false
	}
	return content, false
}

func (tc *TumblrConnector) Download(ctx context.Context, mediaID networkid.MediaID, _ map[string]string) (mediaproxy.GetMediaResponse, error) {
	rawURL, err := parseDirectMediaID(mediaID)
	if err != nil {
		return nil, err
	}
	image, err := tc.directMediaClient.DownloadImage(ctx, rawURL, tc.maxFileSize.Load())
	if err != nil {
		return nil, err
	}
	return &mediaproxy.GetMediaResponseData{
		Reader:        io.NopCloser(bytes.NewReader(image.Data)),
		ContentType:   image.MIMEType,
		ContentLength: int64(len(image.Data)),
	}, nil
}
