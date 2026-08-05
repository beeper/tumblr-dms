package tumblr

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	_ "golang.org/x/image/webp"
)

const imageAcceptHeader = "image/jpeg,image/png,image/webp,image/gif"

const (
	maxVisualHashPixels    = 32_000_000
	maxVisualHashDimension = 32_768
)

// ErrVisualImageHashUnavailable means the image is valid, but its complete
// visual content cannot be decoded safely enough to use as echo identity.
var ErrVisualImageHashUnavailable = errors.New("complete visual image hash is unavailable")

type MediaDownloadFailure uint8

const (
	MediaDownloadTransient MediaDownloadFailure = iota
	MediaDownloadPermanent
)

// MediaDownloadError classifies failures so callers can retry temporary source
// failures without repeatedly retrying media that Tumblr can never serve.
type MediaDownloadError struct {
	Failure MediaDownloadFailure
	message string
}

func (e *MediaDownloadError) Error() string {
	if e == nil || e.message == "" {
		return "tumblr media download failed"
	}
	return e.message
}

func (e *MediaDownloadError) Retryable() bool {
	return e != nil && e.Failure == MediaDownloadTransient
}

func IsPermanentMediaDownloadError(err error) bool {
	var downloadErr *MediaDownloadError
	return errors.As(err, &downloadErr) && downloadErr.Failure == MediaDownloadPermanent
}

func IsTransientMediaDownloadError(err error) bool {
	var downloadErr *MediaDownloadError
	return errors.As(err, &downloadErr) && downloadErr.Failure == MediaDownloadTransient
}

func newMediaDownloadError(failure MediaDownloadFailure, message string) error {
	return &MediaDownloadError{Failure: failure, message: message}
}

type DownloadedImage struct {
	Data         []byte
	MIMEType     string
	SourceDigest string
}

// ImageSourceDigest returns the digest Tumblr places at the start of the
// strong ETag for every rendition of an uploaded image. MD5 is used only to
// correlate a Tumblr CDN response with source bytes from the same account; it
// is never used for authentication, authorization, or general integrity.
func ImageSourceDigest(data []byte) string {
	digest := md5.Sum(data) //nolint:gosec // Tumblr's CDN ETag contract uses MD5.
	return hex.EncodeToString(digest[:])
}

func SniffImageMIME(data []byte) (string, error) {
	var mimeType string
	switch http.DetectContentType(data) {
	case "image/jpeg":
		mimeType = "image/jpeg"
	case "image/png":
		mimeType = "image/png"
	case "image/webp":
		mimeType = "image/webp"
	case "image/gif":
		mimeType = "image/gif"
	default:
		return "", newMediaDownloadError(MediaDownloadPermanent, "image format is not supported")
	}
	return mimeType, nil
}

func CanonicalImageExtension(mimeType string) string {
	switch mimeType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	default:
		return ""
	}
}

// HashVisualImageContent hashes decoded pixels instead of encoded file bytes.
// Tumblr rewrites uploaded image containers even when the visible image is
// unchanged, so encoded-byte hashes cannot safely identify a remote echo.
func HashVisualImageContent(data []byte) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	mimeType, err := SniffImageMIME(data)
	if err != nil {
		return result, err
	}
	if (mimeType == "image/png" && isAnimatedPNG(data)) ||
		(mimeType == "image/webp" && isAnimatedWebP(data)) {
		return result, ErrVisualImageHashUnavailable
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width <= 0 || config.Height <= 0 {
		return result, newMediaDownloadError(MediaDownloadPermanent, "image data could not be decoded")
	}
	pixelCount := uint64(config.Width) * uint64(config.Height)
	if config.Width > maxVisualHashDimension || config.Height > maxVisualHashDimension || pixelCount > maxVisualHashPixels {
		return result, newMediaDownloadError(MediaDownloadPermanent, "image dimensions are too large")
	}
	if format == "gif" {
		// gif.DecodeAll allocates every decoded frame before the caller can
		// enforce an aggregate pixel budget. Use the bounded raw-byte identity
		// path instead of exposing the bridge to compressed animation bombs.
		return result, ErrVisualImageHashUnavailable
	}
	if hasUnsupportedStaticVisualMetadata(data, mimeType) {
		// The registered decoders do not apply EXIF orientation or embedded
		// color profiles. Treat those files as raw-byte identity only instead
		// of claiming two differently rendered images have the same pixels.
		return result, ErrVisualImageHashUnavailable
	}
	decoded, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return result, newMediaDownloadError(MediaDownloadPermanent, "image data could not be decoded")
	}
	bounds := decoded.Bounds()
	if bounds.Dx() != config.Width || bounds.Dy() != config.Height {
		return result, newMediaDownloadError(MediaDownloadPermanent, "decoded image dimensions are inconsistent")
	}

	hasher := sha256.New()
	_, _ = hasher.Write([]byte("tumblr-image-pixels-v2\x00"))
	var dimensions [8]byte
	binary.BigEndian.PutUint32(dimensions[0:4], uint32(config.Width))
	binary.BigEndian.PutUint32(dimensions[4:8], uint32(config.Height))
	_, _ = hasher.Write(dimensions[:])
	hashVisualImagePixels(hasher, decoded)
	copy(result[:], hasher.Sum(nil))
	return result, nil
}

func hashVisualImagePixels(hasher interface{ Write([]byte) (int, error) }, decoded image.Image) {
	bounds := decoded.Bounds()
	row := make([]byte, bounds.Dx()*8)
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			pixel := color.NRGBA64Model.Convert(decoded.At(x, y)).(color.NRGBA64)
			if pixel.A == 0 {
				pixel.R, pixel.G, pixel.B = 0, 0, 0
			}
			offset := (x - bounds.Min.X) * 8
			binary.BigEndian.PutUint16(row[offset:offset+2], pixel.R)
			binary.BigEndian.PutUint16(row[offset+2:offset+4], pixel.G)
			binary.BigEndian.PutUint16(row[offset+4:offset+6], pixel.B)
			binary.BigEndian.PutUint16(row[offset+6:offset+8], pixel.A)
		}
		_, _ = hasher.Write(row)
	}
}

func hasUnsupportedStaticVisualMetadata(data []byte, mimeType string) bool {
	switch mimeType {
	case "image/jpeg":
		return hasUnsupportedJPEGVisualMetadata(data)
	case "image/png":
		return hasUnsupportedPNGVisualMetadata(data)
	case "image/webp":
		return hasUnsupportedWebPVisualMetadata(data)
	default:
		return false
	}
}

func hasUnsupportedJPEGVisualMetadata(data []byte) bool {
	if len(data) < 4 || data[0] != 0xff || data[1] != 0xd8 {
		return true
	}
	for offset := 2; offset < len(data); {
		if data[offset] != 0xff {
			return true
		}
		for offset < len(data) && data[offset] == 0xff {
			offset++
		}
		if offset >= len(data) {
			return true
		}
		marker := data[offset]
		offset++
		switch {
		case marker == 0xd9 || marker == 0xda:
			return false
		case marker == 0x01 || marker == 0xd8 || marker >= 0xd0 && marker <= 0xd7:
			continue
		case marker == 0x00 || offset+2 > len(data):
			return true
		}
		segmentLength := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		if segmentLength < 2 || segmentLength > len(data)-offset {
			return true
		}
		payload := data[offset+2 : offset+segmentLength]
		switch {
		case marker == 0xe2 && bytes.HasPrefix(payload, []byte("ICC_PROFILE\x00")):
			return true
		case marker == 0xe1 && bytes.HasPrefix(payload, []byte("Exif\x00\x00")):
			orientation, present, valid := readEXIFOrientation(payload)
			if !valid || present && orientation != 1 {
				return true
			}
		}
		offset += segmentLength
	}
	return true
}

func hasUnsupportedPNGVisualMetadata(data []byte) bool {
	if len(data) < 8 || !bytes.Equal(data[:8], []byte("\x89PNG\r\n\x1a\n")) {
		return true
	}
	for offset := 8; offset+12 <= len(data); {
		length := uint64(binary.BigEndian.Uint32(data[offset : offset+4]))
		if length > uint64(len(data)-offset-12) {
			return true
		}
		chunkType := string(data[offset+4 : offset+8])
		payloadStart := offset + 8
		payloadEnd := payloadStart + int(length)
		switch chunkType {
		case "iCCP", "cICP", "cHRM", "gAMA", "sRGB", "mDCv", "cLLi":
			return true
		case "eXIf":
			orientation, present, valid := readEXIFOrientation(data[payloadStart:payloadEnd])
			if !valid || present && orientation != 1 {
				return true
			}
		}
		offset += int(length) + 12
		if chunkType == "IEND" {
			return length != 0
		}
	}
	return true
}

func hasUnsupportedWebPVisualMetadata(data []byte) bool {
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WEBP" {
		return true
	}
	declaredEnd := uint64(binary.LittleEndian.Uint32(data[4:8])) + 8
	if declaredEnd < 12 || declaredEnd > uint64(len(data)) {
		return true
	}
	for offset := uint64(12); offset+8 <= declaredEnd; {
		chunkType := string(data[offset : offset+4])
		length := uint64(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		paddedLength := length + length%2
		if paddedLength > declaredEnd-offset-8 {
			return true
		}
		payload := data[offset+8 : offset+8+length]
		switch chunkType {
		case "ICCP", "ANIM", "ANMF":
			return true
		case "VP8X":
			if len(payload) != 10 || payload[0]&(1<<5|1<<3|1<<1) != 0 {
				return true
			}
		case "EXIF":
			orientation, present, valid := readEXIFOrientation(payload)
			if !valid || present && orientation != 1 {
				return true
			}
		}
		offset += 8 + paddedLength
		if offset == declaredEnd {
			return false
		}
	}
	return true
}

func readEXIFOrientation(data []byte) (orientation uint16, present, valid bool) {
	if bytes.HasPrefix(data, []byte("Exif\x00\x00")) {
		data = data[6:]
	}
	if len(data) < 8 {
		return 0, false, false
	}
	var byteOrder binary.ByteOrder
	switch string(data[:2]) {
	case "II":
		byteOrder = binary.LittleEndian
	case "MM":
		byteOrder = binary.BigEndian
	default:
		return 0, false, false
	}
	if byteOrder.Uint16(data[2:4]) != 42 {
		return 0, false, false
	}
	ifdOffset := uint64(byteOrder.Uint32(data[4:8]))
	if ifdOffset < 8 || ifdOffset > uint64(len(data)-2) {
		return 0, false, false
	}
	entryCountOffset := int(ifdOffset)
	entryCount := uint64(byteOrder.Uint16(data[entryCountOffset : entryCountOffset+2]))
	entriesOffset := entryCountOffset + 2
	if entryCount > uint64(len(data)-entriesOffset)/12 {
		return 0, false, false
	}
	for index := uint64(0); index < entryCount; index++ {
		entryOffset := entriesOffset + int(index)*12
		entry := data[entryOffset : entryOffset+12]
		if byteOrder.Uint16(entry[:2]) != 0x0112 {
			continue
		}
		if byteOrder.Uint16(entry[2:4]) != 3 || byteOrder.Uint32(entry[4:8]) != 1 {
			return 0, false, false
		}
		orientation = byteOrder.Uint16(entry[8:10])
		if orientation < 1 || orientation > 8 {
			return 0, false, false
		}
		return orientation, true, true
	}
	return 0, false, true
}

func isAnimatedPNG(data []byte) bool {
	if len(data) < 8 || !bytes.Equal(data[:8], []byte("\x89PNG\r\n\x1a\n")) {
		return false
	}
	for offset := 8; offset+12 <= len(data); {
		length := uint64(binary.BigEndian.Uint32(data[offset : offset+4]))
		remaining := uint64(len(data) - offset - 12)
		if length > remaining {
			return false
		}
		chunkType := string(data[offset+4 : offset+8])
		if chunkType == "acTL" {
			return true
		}
		offset += int(length) + 12
		if chunkType == "IEND" {
			return false
		}
	}
	return false
}

func isAnimatedWebP(data []byte) bool {
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WEBP" {
		return false
	}
	declaredEnd := uint64(binary.LittleEndian.Uint32(data[4:8])) + 8
	if declaredEnd > uint64(len(data)) {
		declaredEnd = uint64(len(data))
	}
	for offset := uint64(12); offset+8 <= declaredEnd; {
		chunkType := string(data[offset : offset+4])
		length := uint64(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		if chunkType == "ANIM" || chunkType == "ANMF" {
			return true
		}
		paddedLength := length + length%2
		if paddedLength > declaredEnd-offset-8 {
			return false
		}
		offset += 8 + paddedLength
	}
	return false
}

func (c *Client) DownloadImage(ctx context.Context, rawURL string, maxBytes int64) (DownloadedImage, error) {
	if c == nil {
		return DownloadedImage{}, fmt.Errorf("tumblr client is not available for image download")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxDownloadBytes
	}
	downloadURL, err := normalizeDownloadURL(rawURL)
	if err != nil {
		return DownloadedImage{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return DownloadedImage{}, newMediaDownloadError(MediaDownloadPermanent, "download request is invalid")
	}
	setAnonymousImageHeaders(req, c.userAgent)
	resp, err := c.downloadHTTPClient().Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if ctx.Err() != nil {
			return DownloadedImage{}, ctx.Err()
		}
		var downloadErr *MediaDownloadError
		if errors.As(err, &downloadErr) {
			return DownloadedImage{}, downloadErr
		}
		return DownloadedImage{}, newMediaDownloadError(MediaDownloadTransient, "download request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		failure := MediaDownloadTransient
		if resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError &&
			resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests {
			failure = MediaDownloadPermanent
		}
		return DownloadedImage{}, newMediaDownloadError(failure, fmt.Sprintf("download failed with HTTP %d", resp.StatusCode))
	}
	if resp.ContentLength > maxBytes {
		return DownloadedImage{}, newMediaDownloadError(MediaDownloadPermanent, "download is too large")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return DownloadedImage{}, ctx.Err()
		}
		return DownloadedImage{}, newMediaDownloadError(MediaDownloadTransient, "download read failed")
	}
	if int64(len(body)) > maxBytes {
		return DownloadedImage{}, newMediaDownloadError(MediaDownloadPermanent, "download is too large")
	}
	mimeType, err := SniffImageMIME(body)
	if err != nil {
		return DownloadedImage{}, err
	}
	sourceDigest := tumblrSourceDigestFromETag(resp.Header, resp.Request)
	return DownloadedImage{Data: body, MIMEType: mimeType, SourceDigest: sourceDigest}, nil
}

func tumblrSourceDigestFromETag(headers http.Header, request *http.Request) string {
	if request == nil || request.URL == nil || !isTrustedTumblrMediaHost(strings.ToLower(request.URL.Hostname())) {
		return ""
	}
	values := headers.Values("ETag")
	if len(values) != 1 {
		return ""
	}
	value := strings.TrimSpace(values[0])
	if len(value) < 2 || strings.HasPrefix(value, "W/") || value[0] != '"' || value[len(value)-1] != '"' {
		return ""
	}
	parts := strings.Split(value[1:len(value)-1], "-")
	if len(parts) != 3 || len(parts[0]) != md5.Size*2 || parts[1] == "" || parts[2] == "" {
		return ""
	}
	for _, char := range parts[1] {
		if char < '0' || char > '9' {
			return ""
		}
	}
	// Tumblr's opaque suffix may contain an odd number of hex digits, so
	// validate individual nibbles instead of decoding complete bytes.
	for _, value := range []string{parts[0], parts[2]} {
		for _, char := range value {
			if (char < '0' || char > '9') && (char < 'a' || char > 'f') && (char < 'A' || char > 'F') {
				return ""
			}
		}
	}
	return strings.ToLower(parts[0])
}

func (c *Client) Download(ctx context.Context, rawURL string, maxBytes int64) ([]byte, error) {
	downloaded, err := c.DownloadImage(ctx, rawURL, maxBytes)
	if err != nil {
		return nil, err
	}
	return downloaded.Data, nil
}

func (c *Client) downloadHTTPClient() *http.Client {
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *httpClient
	clone.Jar = nil
	existingCheckRedirect := clone.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req == nil || req.URL == nil {
			return newMediaDownloadError(MediaDownloadPermanent, "download redirect URL is invalid")
		}
		if len(via) >= 10 {
			return newMediaDownloadError(MediaDownloadPermanent, "download has too many redirects")
		}
		if existingCheckRedirect != nil {
			if err := existingCheckRedirect(req, via); err != nil {
				if errors.Is(err, http.ErrUseLastResponse) {
					return err
				}
				return newMediaDownloadError(MediaDownloadPermanent, "download redirect was rejected")
			}
		}
		normalized, err := normalizeDownloadURL(req.URL.String())
		if err != nil {
			return err
		}
		req.URL, err = url.Parse(normalized)
		if err != nil {
			return newMediaDownloadError(MediaDownloadPermanent, "download redirect URL is invalid")
		}
		req.Host = ""
		setAnonymousImageHeaders(req, c.userAgent)
		return nil
	}
	return &clone
}

func setAnonymousImageHeaders(req *http.Request, userAgent string) {
	req.Header = make(http.Header, 2)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", imageAcceptHeader)
}

func normalizeDownloadURL(rawURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Opaque != "" {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL is invalid")
	}
	if !strings.EqualFold(parsed.Scheme, "https") {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL must use https")
	}
	if parsed.Host == "" {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL host is missing")
	}
	if parsed.User != nil {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL user info is not allowed")
	}
	if parsed.Fragment != "" {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL fragments are not allowed")
	}
	port := parsed.Port()
	if port != "" && port != "443" {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL port is not allowed")
	}
	host := strings.ToLower(parsed.Hostname())
	if !isCanonicalASCIIDomain(host) {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL host is invalid")
	}
	if !isTrustedTumblrDownloadHost(host) {
		return "", newMediaDownloadError(MediaDownloadPermanent, "download URL host is not allowed")
	}
	parsed.Scheme = "https"
	parsed.Host = host
	parsed.ForceQuery = false
	return parsed.String(), nil
}

// IsDownloadURLAllowed reports whether DownloadImage would accept rawURL before making a request.
func IsDownloadURLAllowed(rawURL string) bool {
	_, err := normalizeDownloadURL(rawURL)
	return err == nil
}

func isTrustedTumblrDownloadHost(host string) bool {
	return host == "tumblr.com" || strings.HasSuffix(host, ".tumblr.com")
}

func isTrustedTumblrMediaHost(host string) bool {
	return host == "media.tumblr.com" || strings.HasSuffix(host, ".media.tumblr.com")
}

func isCanonicalASCIIDomain(host string) bool {
	if host == "" || len(host) > 253 || strings.HasSuffix(host, ".") || strings.IndexFunc(host, func(r rune) bool {
		return r > unicode.MaxASCII
	}) >= 0 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}
