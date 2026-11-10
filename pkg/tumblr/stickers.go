package tumblr

import (
	"context"
	"errors"
	"net/http"
)

type StickerPack struct {
	ID          string       `json:"id"`
	Title       string       `json:"title"`
	Description string       `json:"description"`
	Image       MessageImage `json:"image"`
	Stickers    []Sticker    `json:"stickers"`
}

type Sticker struct {
	ID          string       `json:"id"`
	Description string       `json:"description"`
	Image       MessageImage `json:"image"`
}

func (c *Client) GetStickerPacks(ctx context.Context) ([]StickerPack, error) {
	var resp struct {
		StickerPacks *[]StickerPack `json:"stickerPacks"`
	}
	if err := c.do(ctx, http.MethodGet, "/v2/stickerpacks", nil, nil, &resp); err != nil {
		return nil, err
	}
	if resp.StickerPacks == nil {
		return nil, errors.New("invalid Tumblr sticker pack list")
	}
	return *resp.StickerPacks, nil
}
