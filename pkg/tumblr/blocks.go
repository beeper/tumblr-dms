package tumblr

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

var blogUUIDPattern = regexp.MustCompile(`^t:[A-Za-z0-9_-]+$`)

func IsBlogUUID(value string) bool {
	return blogUUIDPattern.MatchString(value)
}

func (c *Client) SetBlogBlocked(ctx context.Context, owner, peer string, blocked bool) error {
	if !IsBlogUUID(owner) || !IsBlogUUID(peer) || owner == peer {
		return fmt.Errorf("invalid Tumblr block owner or target")
	}
	method := http.MethodDelete
	if blocked {
		method = http.MethodPost
	}
	return c.do(ctx, method, "/v2/blog/"+url.PathEscape(owner)+"/blocks", nil, map[string]string{"blocked_tumblelog": peer}, nil)
}

func (c *Client) ListBlockedBlogs(ctx context.Context, owner string) ([]Blog, error) {
	if !IsBlogUUID(owner) {
		return nil, fmt.Errorf("invalid Tumblr block list owner UUID")
	}
	var blogs []Blog
	seenIDs := make(map[string]bool)
	for offset := 0; ; {
		var page struct {
			Blogs *[]Blog        `json:"blockedTumblelogs"`
			Links *ResponseLinks `json:"links"`
			Other *ResponseLinks `json:"_links"`
		}
		err := c.do(ctx, http.MethodGet, "/v2/blog/"+url.PathEscape(owner)+"/blocks", url.Values{
			"limit": {"20"}, "offset": {strconv.Itoa(offset)},
		}, nil, &page)
		if err != nil {
			return nil, err
		}
		if page.Blogs == nil || len(*page.Blogs) > 20 || page.Links != nil && page.Other != nil {
			return nil, fmt.Errorf("invalid Tumblr block list")
		}
		for _, blog := range *page.Blogs {
			if !IsBlogUUID(blog.UUID) || blog.UUID == owner || seenIDs[blog.UUID] {
				return nil, fmt.Errorf("invalid or repeated Tumblr blocked blog")
			}
			seenIDs[blog.UUID] = true
			blogs = append(blogs, blog)
		}
		links := page.Links
		if links == nil {
			links = page.Other
		}
		next := offset + len(*page.Blogs)
		if links != nil && links.Next != nil {
			if links.Next.Method != "" && links.Next.Method != http.MethodGet {
				return nil, fmt.Errorf("invalid Tumblr block list continuation method")
			}
			value := links.Next.QueryParam("offset")
			if value == "" {
				parsed, parseErr := url.Parse(links.Next.Href)
				if parseErr != nil {
					return nil, fmt.Errorf("invalid Tumblr block list continuation")
				}
				value = parsed.Query().Get("offset")
			}
			parsed, parseErr := strconv.Atoi(value)
			if parseErr != nil || parsed != next || next <= offset {
				return nil, fmt.Errorf("non-contiguous Tumblr block list continuation")
			}
		} else if len(*page.Blogs) < 20 {
			return blogs, nil
		}
		offset = next
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
