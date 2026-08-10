package tumblr

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/bits"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	MessageTypeText    = "TEXT"
	MessageTypeImage   = "IMAGE"
	MessageTypeSticker = "STICKER"
	MessageTypePostRef = "POSTREF"
)

type APIEnvelope struct {
	Meta     APIMeta    `json:"meta"`
	Response any        `json:"response,omitempty"`
	Errors   []APIError `json:"errors,omitempty"`
}

type APIMeta struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`
}

type APIError struct {
	Title  string `json:"title"`
	Code   int    `json:"code"`
	Detail string `json:"detail"`
	Logout bool   `json:"logout"`
}

type UserInfoResponse struct {
	User  *User  `json:"user"`
	Blogs []Blog `json:"blogs"`
}

type User struct {
	Name  string `json:"name"`
	Title string `json:"title"`
	Blogs []Blog `json:"blogs"`
}

type BlogInfoResponse struct {
	Blog *Blog `json:"blog"`
}

func (r *BlogInfoResponse) UnmarshalJSON(data []byte) error {
	var wrapped struct {
		Blog *Blog `json:"blog"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && wrapped.Blog != nil {
		r.Blog = wrapped.Blog
		return nil
	}
	var blog Blog
	if err := json.Unmarshal(data, &blog); err != nil {
		return err
	}
	r.Blog = &blog
	return nil
}

type Blog struct {
	UUID        string       `json:"uuid"`
	Name        string       `json:"name"`
	Title       string       `json:"title"`
	URL         string       `json:"url"`
	BlogViewURL string       `json:"blogViewUrl"`
	CanMessage  bool         `json:"canMessage"`
	Primary     bool         `json:"primary"`
	IsAdult     bool         `json:"isAdult"`
	Avatar      []ImageAsset `json:"avatar"`
	Theme       *BlogTheme   `json:"theme"`
}

func (b *Blog) UnmarshalJSON(data []byte) error {
	type blogAlias Blog
	var alias blogAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	*b = Blog(alias)
	var extra struct {
		BlogViewURL string `json:"blog_view_url"`
		CanMessage  *bool  `json:"can_message"`
		IsAdult     *bool  `json:"is_adult"`
	}
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	if strings.TrimSpace(extra.BlogViewURL) != "" {
		b.BlogViewURL = extra.BlogViewURL
	}
	if extra.CanMessage != nil {
		b.CanMessage = *extra.CanMessage
	}
	if extra.IsAdult != nil {
		b.IsAdult = *extra.IsAdult
	}
	return nil
}

type BlogTheme struct {
	AvatarShape string `json:"avatarShape"`
}

func (t *BlogTheme) UnmarshalJSON(data []byte) error {
	type themeAlias BlogTheme
	var alias themeAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	*t = BlogTheme(alias)
	var extra struct {
		AvatarShape string `json:"avatar_shape"`
	}
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	if strings.TrimSpace(extra.AvatarShape) != "" {
		t.AvatarShape = extra.AvatarShape
	}
	return nil
}

type ImageAsset struct {
	URL    string `json:"url"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

type ConversationListResponse struct {
	Conversations []Conversation `json:"conversations"`
	Links         ResponseLinks  `json:"links"`
}

type ParticipantSuggestionsResponse struct {
	Blogs []Blog `json:"blogs"`
}

func (r *ParticipantSuggestionsResponse) UnmarshalJSON(data []byte) error {
	var direct []Blog
	if err := json.Unmarshal(data, &direct); err == nil {
		r.Blogs = direct
		return nil
	}
	var wrapped struct {
		Blogs        *[]Blog `json:"blogs"`
		Participants *[]Blog `json:"participants"`
		Suggestions  *[]Blog `json:"suggestions"`
		Results      *[]Blog `json:"results"`
		Data         *[]Blog `json:"data"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return err
	}
	switch {
	case wrapped.Blogs != nil:
		r.Blogs = *wrapped.Blogs
	case wrapped.Participants != nil:
		r.Blogs = *wrapped.Participants
	case wrapped.Suggestions != nil:
		r.Blogs = *wrapped.Suggestions
	case wrapped.Results != nil:
		r.Blogs = *wrapped.Results
	case wrapped.Data != nil:
		r.Blogs = *wrapped.Data
	}
	return nil
}

func (r *ConversationListResponse) UnmarshalJSON(data []byte) error {
	var direct []Conversation
	if err := json.Unmarshal(data, &direct); err == nil {
		r.Conversations = direct
		return nil
	}
	var wrapped struct {
		Conversations *[]Conversation `json:"conversations"`
		Data          *[]Conversation `json:"data"`
		Links         *ResponseLinks  `json:"links"`
		Underscore    *ResponseLinks  `json:"_links"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return err
	}
	if wrapped.Conversations != nil {
		r.Conversations = *wrapped.Conversations
	} else if wrapped.Data != nil {
		r.Conversations = *wrapped.Data
	}
	switch {
	case wrapped.Links != nil:
		r.Links = *wrapped.Links
	case wrapped.Underscore != nil:
		r.Links = *wrapped.Underscore
	default:
		r.Links = ResponseLinks{}
	}
	return nil
}

type ConversationMessagesResponse struct {
	Conversation *Conversation `json:"conversation"`
	Messages     []Message     `json:"messages"`
	Links        ResponseLinks `json:"links"`
}

func (r *ConversationMessagesResponse) UnmarshalJSON(data []byte) error {
	var wrapped struct {
		Conversation *Conversation  `json:"conversation"`
		Messages     *[]Message     `json:"messages"`
		Data         *[]Message     `json:"data"`
		Links        *ResponseLinks `json:"links"`
		Underscore   *ResponseLinks `json:"_links"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && (wrapped.Conversation != nil || wrapped.Messages != nil || wrapped.Data != nil) {
		r.Conversation = wrapped.Conversation
		r.Links = linksFromWrapped(wrapped.Links, wrapped.Underscore)
		switch {
		case wrapped.Messages != nil:
			r.Messages = *wrapped.Messages
		case wrapped.Data != nil:
			r.Messages = *wrapped.Data
		case r.Conversation != nil:
			r.Messages = r.Conversation.Messages.Data
		}
		if r.Conversation != nil {
			if r.Links.Next == nil && r.Conversation.Messages.Links.Next != nil {
				r.Links = r.Conversation.Messages.Links
			}
			r.Messages = deriveMissingMessageIDs(r.Conversation.ID, r.Conversation.Participants, r.Messages)
			if len(r.Conversation.Messages.Data) > 0 {
				r.Conversation.Messages.Data = deriveMissingMessageIDs(
					r.Conversation.ID,
					r.Conversation.Participants,
					r.Conversation.Messages.Data,
				)
			}
		}
		return nil
	}
	var conversation Conversation
	if err := json.Unmarshal(data, &conversation); err != nil {
		return err
	}
	r.Conversation = &conversation
	r.Messages = conversation.Messages.Data
	r.Links = conversation.Messages.Links
	return nil
}

type ResponseLinks struct {
	Next *ResponseLink `json:"next"`
	Prev *ResponseLink `json:"prev"`
}

type ResponseLink struct {
	Href        string            `json:"href"`
	Method      string            `json:"method"`
	QueryParams map[string]string `json:"queryParams"`
}

func (l *ResponseLink) UnmarshalJSON(data []byte) error {
	var raw struct {
		Href        string          `json:"href"`
		Method      string          `json:"method"`
		QueryParams json.RawMessage `json:"queryParams"`
		QuerySnake  json.RawMessage `json:"query_params"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	l.Href = raw.Href
	l.Method = raw.Method
	l.QueryParams = decodeQueryParams(raw.QueryParams)
	if len(l.QueryParams) == 0 {
		l.QueryParams = decodeQueryParams(raw.QuerySnake)
	}
	return nil
}

func (l ResponseLink) QueryParam(name string) string {
	if l.QueryParams == nil {
		return ""
	}
	return strings.TrimSpace(l.QueryParams[name])
}

func (r ConversationListResponse) NextBefore() (string, error) {
	return nextBeforeCursor(r.Links.Next)
}

func (r ConversationMessagesResponse) NextBefore() (string, error) {
	return nextBeforeCursor(r.Links.Next)
}

func nextBeforeCursor(next *ResponseLink) (string, error) {
	if next == nil {
		return "", nil
	}
	if before := next.QueryParam("before"); before != "" {
		return before, nil
	}
	if href := strings.TrimSpace(next.Href); href != "" {
		parsed, err := url.Parse(href)
		if err != nil {
			return "", fmt.Errorf("invalid Tumblr next-page URL: %w", err)
		}
		if before := strings.TrimSpace(parsed.Query().Get("before")); before != "" {
			return before, nil
		}
	}
	return "", fmt.Errorf("tumblr next-page link is missing its before cursor")
}

func linksFromWrapped(links, underscore *ResponseLinks) ResponseLinks {
	switch {
	case links != nil:
		return *links
	case underscore != nil:
		return *underscore
	default:
		return ResponseLinks{}
	}
}

func decodeQueryParams(data json.RawMessage) map[string]string {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	var direct map[string]string
	if err := json.Unmarshal(data, &direct); err == nil {
		return direct
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		if text := decodeFlexibleString(value); text != "" {
			out[key] = text
			continue
		}
		var list []json.RawMessage
		if err := json.Unmarshal(value, &list); err == nil {
			for _, item := range list {
				if text := decodeFlexibleString(item); text != "" {
					out[key] = text
					break
				}
			}
		}
	}
	return out
}

type Conversation struct {
	ID                    string      `json:"id"`
	Participants          []Blog      `json:"participants"`
	Messages              MessagePage `json:"messages"`
	UnreadMessagesCount   int         `json:"unreadMessagesCount"`
	LastReadTimestamp     int64       `json:"lastReadTs,omitempty"`
	LastModifiedTimestamp int64       `json:"lastModifiedTs,omitempty"`
	LastUpdated           *time.Time  `json:"lastUpdated,omitempty"`
}

func (c *Conversation) UnmarshalJSON(data []byte) error {
	type conversationAlias Conversation
	var alias conversationAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	*c = Conversation(alias)
	var extra struct {
		UnreadMessagesCount   json.RawMessage `json:"unread_messages_count"`
		LastReadTimestamp     json.RawMessage `json:"last_read_ts"`
		LastModifiedTimestamp json.RawMessage `json:"last_modified_ts"`
	}
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	if value, ok, err := decodeOptionalFlexibleInt64(extra.UnreadMessagesCount); err != nil {
		return fmt.Errorf("decode unread_messages_count: %w", err)
	} else if ok {
		c.UnreadMessagesCount = int(value)
	}
	if value, ok, err := decodeOptionalFlexibleInt64(extra.LastReadTimestamp); err != nil {
		return fmt.Errorf("decode last_read_ts: %w", err)
	} else if ok {
		c.LastReadTimestamp = value
	}
	if value, ok, err := decodeOptionalFlexibleInt64(extra.LastModifiedTimestamp); err != nil {
		return fmt.Errorf("decode last_modified_ts: %w", err)
	} else if ok {
		c.LastModifiedTimestamp = value
	}
	c.Messages.Data = deriveMissingMessageIDs(c.ID, c.Participants, c.Messages.Data)
	return nil
}

func decodeOptionalFlexibleInt64(data json.RawMessage) (int64, bool, error) {
	if len(data) == 0 || string(data) == "null" {
		return 0, false, nil
	}
	value, err := decodeFlexibleInt64(data)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

type MessagePage struct {
	Data  []Message     `json:"data"`
	Links ResponseLinks `json:"links"`
}

func (p *MessagePage) UnmarshalJSON(data []byte) error {
	var direct []Message
	if err := json.Unmarshal(data, &direct); err == nil {
		p.Data = direct
		p.Links = ResponseLinks{}
		return nil
	}
	var wrapped struct {
		Data       []Message      `json:"data"`
		Links      *ResponseLinks `json:"links"`
		Underscore *ResponseLinks `json:"_links"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return err
	}
	p.Data = wrapped.Data
	p.Links = linksFromWrapped(wrapped.Links, wrapped.Underscore)
	return nil
}

type Message struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Context     string          `json:"context,omitempty"`
	Participant *Blog           `json:"participant"`
	Content     *MessageContent `json:"content"`
	Post        *PostRef        `json:"post"`
	Images      []MessageImage  `json:"images"`
	Timestamp   int64           `json:"timestamp"`
	Raw         json.RawMessage `json:"-"`
}

func (m *Message) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID          string          `json:"id"`
		Type        string          `json:"type"`
		Context     string          `json:"context"`
		Participant json.RawMessage `json:"participant"`
		Content     *MessageContent `json:"content"`
		Post        *PostRef        `json:"post"`
		Images      []MessageImage  `json:"images"`
		Timestamp   json.RawMessage `json:"timestamp"`
		TS          json.RawMessage `json:"ts"`
		Message     string          `json:"message"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	m.Raw = append(m.Raw[:0], data...)
	m.ID = strings.TrimSpace(raw.ID)
	m.Type = strings.TrimSpace(raw.Type)
	m.Context = strings.TrimSpace(raw.Context)
	m.Participant = nil
	if len(raw.Participant) > 0 && string(raw.Participant) != "null" {
		participant, err := decodeMessageParticipant(raw.Participant)
		if err != nil {
			return err
		}
		m.Participant = participant
	}
	m.Content = raw.Content
	if m.Content == nil && strings.EqualFold(m.Type, MessageTypeText) && strings.TrimSpace(raw.Message) != "" {
		m.Content = &MessageContent{Text: raw.Message}
	}
	m.Post = raw.Post
	m.Images = raw.Images
	m.Timestamp = 0
	if len(raw.Timestamp) > 0 && string(raw.Timestamp) != "null" {
		timestamp, err := decodeFlexibleInt64(raw.Timestamp)
		if err != nil {
			return fmt.Errorf("decode timestamp: %w", err)
		}
		m.Timestamp = timestamp
	} else if len(raw.TS) > 0 && string(raw.TS) != "null" {
		timestamp, err := decodeFlexibleInt64(raw.TS)
		if err != nil {
			return fmt.Errorf("decode ts: %w", err)
		}
		m.Timestamp = timestamp
	}
	return nil
}

func decodeMessageParticipant(data json.RawMessage) (*Blog, error) {
	var blog Blog
	if err := json.Unmarshal(data, &blog); err == nil {
		return &blog, nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if strings.HasPrefix(value, "<BLOG_") && strings.HasSuffix(value, ">") {
		return &Blog{Name: value}, nil
	}
	if strings.HasPrefix(value, "<UUID_") && strings.HasSuffix(value, ">") {
		return &Blog{UUID: value}, nil
	}
	if name := NormalizeBlogName(value); name != "" {
		return &Blog{Name: name}, nil
	}
	return &Blog{UUID: value}, nil
}

func decodeFlexibleInt64(data json.RawMessage) (int64, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return 0, err
	}
	switch typed := value.(type) {
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			return parsed, nil
		}
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		return int64(parsed), err
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}

func decodeFlexibleString(data json.RawMessage) string {
	if len(data) == 0 || string(data) == "null" {
		return ""
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return strings.TrimSpace(typed.String())
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func deriveMissingMessageIDs(conversationID string, participants []Blog, messages []Message) []Message {
	for i := range messages {
		if strings.TrimSpace(messages[i].ID) == "" {
			messages[i].ID = syntheticMessageID(conversationID, participants, messages[i])
		}
	}
	return messages
}

func syntheticMessageID(conversationID string, participants []Blog, message Message) string {
	conversationID = strings.TrimSpace(conversationID)
	participant := messageParticipantID(message.Participant, participants)
	content := messageContentID(message)
	if conversationID == "" || participant == "" || strings.TrimSpace(message.Type) == "" || message.Timestamp <= 0 || content == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(strings.Join([]string{
		conversationID,
		participant,
		strconv.FormatInt(canonicalMessageTimestamp(message.Timestamp), 10),
		strings.ToUpper(strings.TrimSpace(message.Type)),
		content,
	}, "\x00")))
	return "synthetic-" + fmt.Sprintf("%x", hash[:16])
}

func canonicalMessageTimestamp(timestamp int64) int64 {
	if timestamp > 0 && timestamp <= 1_000_000_000_000 {
		return timestamp * 1_000
	}
	return timestamp
}

func messageParticipantID(participant *Blog, participants []Blog) string {
	if participant == nil {
		return ""
	}
	if participant.UUID != "" {
		return strings.TrimSpace(participant.UUID)
	}
	rawName := strings.TrimSpace(participant.Name)
	name := NormalizeBlogName(rawName)
	if name == "" {
		return rawName
	}
	for _, candidate := range participants {
		candidateUUID := strings.TrimSpace(candidate.UUID)
		if candidateUUID != "" && (strings.EqualFold(NormalizeBlogName(candidate.Name), name) || strings.EqualFold(candidateUUID, rawName)) {
			return candidateUUID
		}
	}
	return name
}

func messageContentID(message Message) string {
	if message.Content != nil && strings.TrimSpace(message.Content.Text) != "" {
		return "text:" + strings.TrimSpace(message.Content.Text)
	}
	if message.Post != nil && strings.TrimSpace(message.Post.ID) != "" {
		return "post:" + strings.TrimSpace(message.Post.ID)
	}
	if len(message.Images) > 0 {
		images := make([]string, 0, len(message.Images))
		for _, image := range message.Images {
			imageURL := canonicalMessageImageURL(image)
			if imageURL == "" {
				// Do not build an identity from only part of a multi-image
				// message. The raw payload fallback below is safer than
				// collapsing two messages that differ in the omitted image.
				images = nil
				break
			}
			images = append(images, imageURL)
		}
		if len(images) > 0 {
			sort.Strings(images)
			unique := images[:0]
			for _, imageURL := range images {
				if len(unique) == 0 || unique[len(unique)-1] != imageURL {
					unique = append(unique, imageURL)
				}
			}
			return "images:" + strings.Join(unique, "\x00")
		}
	}
	if len(message.Raw) > 0 {
		hash := sha256.Sum256(message.Raw)
		return "raw:" + fmt.Sprintf("%x", hash[:16])
	}
	return ""
}

func canonicalMessageImageURL(image MessageImage) string {
	if original, err := normalizeDownloadURL(strings.TrimSpace(image.OriginalSize.URL)); err == nil {
		return original
	}
	altURLs := make([]string, 0, len(image.AltSizes))
	for _, alt := range image.AltSizes {
		if altURL, err := normalizeDownloadURL(strings.TrimSpace(alt.URL)); err == nil {
			altURLs = append(altURLs, altURL)
		}
	}
	if len(altURLs) == 0 {
		return ""
	}
	sort.Strings(altURLs)
	return altURLs[0]
}

type MessageContent struct {
	Text string `json:"text"`
}

type MessageImage struct {
	OriginalSize ImageAsset   `json:"originalSize"`
	AltSizes     []ImageAsset `json:"altSizes"`
}

func (image MessageImage) Candidates() []ImageAsset {
	altSizes := append([]ImageAsset(nil), image.AltSizes...)
	sort.SliceStable(altSizes, func(i, j int) bool {
		iHigh, iLow := imageAssetArea(altSizes[i])
		jHigh, jLow := imageAssetArea(altSizes[j])
		if iHigh != jHigh {
			return iHigh > jHigh
		}
		return iLow > jLow
	})

	candidates := make([]ImageAsset, 0, 1+len(altSizes))
	seenURLs := make(map[string]struct{}, 1+len(altSizes))
	appendCandidate := func(asset ImageAsset) {
		asset.URL = strings.TrimSpace(asset.URL)
		if asset.URL == "" {
			return
		}
		if _, seen := seenURLs[asset.URL]; seen {
			return
		}
		seenURLs[asset.URL] = struct{}{}
		candidates = append(candidates, asset)
	}
	appendCandidate(image.OriginalSize)
	for _, asset := range altSizes {
		appendCandidate(asset)
	}
	return candidates
}

func imageAssetArea(asset ImageAsset) (high, low uint64) {
	if asset.Width <= 0 || asset.Height <= 0 {
		return 0, 0
	}
	return bits.Mul64(uint64(asset.Width), uint64(asset.Height))
}

func (m Message) BestImage() *ImageAsset {
	for _, image := range m.Images {
		if candidates := image.Candidates(); len(candidates) > 0 {
			return &candidates[0]
		}
	}
	return nil
}

type PostRef struct {
	ID               string                 `json:"id"`
	Summary          string                 `json:"summary"`
	State            string                 `json:"state,omitempty"`
	IsNSFW           bool                   `json:"isNsfw,omitempty"`
	IsGIF            bool                   `json:"isGif,omitempty"`
	HasIsGIF         bool                   `json:"-"`
	ObjectType       string                 `json:"objectType,omitempty"`
	Type             string                 `json:"type,omitempty"`
	Classification   string                 `json:"classification,omitempty"`
	CanSendInMessage bool                   `json:"canSendInMessage,omitempty"`
	URL              string                 `json:"url,omitempty"`
	PostURL          string                 `json:"postUrl,omitempty"`
	Blog             PostRefBlog            `json:"blog,omitempty"`
	Content          []PostRefContent       `json:"content,omitempty"`
	CommunityLabels  PostRefCommunityLabels `json:"communityLabels,omitempty"`
}

func (p *PostRef) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID               json.RawMessage        `json:"id"`
		Summary          string                 `json:"summary"`
		State            string                 `json:"state"`
		IsNSFW           bool                   `json:"isNsfw"`
		IsGIF            *bool                  `json:"isGif"`
		ObjectType       string                 `json:"objectType"`
		Type             string                 `json:"type"`
		Classification   string                 `json:"classification"`
		CanSendInMessage bool                   `json:"canSendInMessage"`
		URL              string                 `json:"url"`
		PostURL          string                 `json:"postUrl"`
		PostURLSnake     string                 `json:"post_url"`
		Blog             json.RawMessage        `json:"blog"`
		Content          []PostRefContent       `json:"content"`
		CommunityLabels  PostRefCommunityLabels `json:"communityLabels"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	p.ID = decodeFlexibleString(raw.ID)
	p.Summary = raw.Summary
	p.State = strings.TrimSpace(raw.State)
	p.IsNSFW = raw.IsNSFW
	p.HasIsGIF = raw.IsGIF != nil
	p.IsGIF = p.HasIsGIF && *raw.IsGIF
	p.ObjectType = strings.TrimSpace(raw.ObjectType)
	p.Type = strings.TrimSpace(raw.Type)
	p.Classification = strings.TrimSpace(raw.Classification)
	p.CanSendInMessage = raw.CanSendInMessage
	p.URL = strings.TrimSpace(raw.URL)
	p.PostURL = strings.TrimSpace(firstNonEmpty(raw.PostURL, raw.PostURLSnake))
	p.Content = raw.Content
	p.CommunityLabels = raw.CommunityLabels
	p.Blog = PostRefBlog{}
	if len(raw.Blog) > 0 && string(raw.Blog) != "null" {
		blog, err := decodePostRefBlog(raw.Blog)
		if err != nil {
			return err
		}
		p.Blog = blog
	}
	return nil
}

func (p PostRef) BestURL() string {
	return strings.TrimSpace(firstNonEmpty(p.PostURL, p.URL, p.Blog.URL, p.Blog.BlogViewURL))
}

func (p PostRef) IsUnavailable() bool {
	return strings.EqualFold(strings.TrimSpace(p.State), "disabled") || p.IsNSFW
}

const maxPostRefGIFCandidates = 16

const MessageContextGIF = "messaging-gif"

type PostRefGIFMode uint8

const (
	PostRefGIFOrdinary PostRefGIFMode = iota
	PostRefGIFKnown
	PostRefGIFProbe
)

type PostRefContent struct {
	Type  string         `json:"type"`
	Text  string         `json:"text,omitempty"`
	Media []PostRefMedia `json:"media"`
}

type PostRefMedia struct {
	URL     string      `json:"url"`
	Type    string      `json:"type"`
	Width   int         `json:"width"`
	Height  int         `json:"height"`
	Cropped bool        `json:"cropped,omitempty"`
	Poster  *ImageAsset `json:"poster,omitempty"`
}

type PostRefCommunityLabels struct {
	HasCommunityLabel bool `json:"hasCommunityLabel"`
}

// PostRefGIFMode reports whether a post reference is known to be a GIF, may be
// probed as a GIF, or must remain an ordinary shared post. Tumblr's hydrated
// conversation payloads may omit or mislabel GIF metadata, so eligible media
// candidates are probed and downloaded bytes remain the authority.
func (m Message) PostRefGIFMode() PostRefGIFMode {
	if !strings.EqualFold(strings.TrimSpace(m.Type), MessageTypePostRef) || m.Post == nil {
		return PostRefGIFOrdinary
	}
	if (m.Post.HasIsGIF && m.Post.IsGIF) || strings.EqualFold(strings.TrimSpace(m.Context), MessageContextGIF) {
		return PostRefGIFKnown
	}
	if len(m.Post.GIFPreviewCandidates()) > 0 {
		return PostRefGIFProbe
	}
	return PostRefGIFOrdinary
}

// GIFPreviewCandidates returns Tumblr-hosted, uncropped renditions that may
// contain GIF bytes. Tumblr wraps GIFs in posts, may attach arbitrary source-
// post text blocks, and may label actual GIF bytes as image/webp. Those wrapper
// fields are never part of the Matrix media message. Text authored in the DM
// composer arrives as a separate TEXT message and is preserved by the normal
// text converter. The downloaded bytes remain the final authority.
func (p *PostRef) GIFPreviewCandidates() []ImageAsset {
	if p == nil || p.IsUnavailable() || p.Blog.IsAdult || p.Blog.ShouldBlur ||
		p.CommunityLabels.HasCommunityLabel || !p.CanSendInMessage ||
		!strings.EqualFold(p.ObjectType, "post") || !strings.EqualFold(p.Type, "blocks") ||
		!strings.EqualFold(p.State, "published") ||
		!strings.EqualFold(p.Classification, "clean") {
		return nil
	}

	var media []PostRefMedia
	for _, block := range p.Content {
		if strings.EqualFold(strings.TrimSpace(block.Type), "image") && len(block.Media) > 0 {
			media = block.Media
			break
		}
	}
	if len(media) == 0 {
		return nil
	}
	candidates := make([]ImageAsset, 0, len(media))
	seenURLs := make(map[string]struct{}, len(media))
	for _, item := range media {
		if item.Cropped || item.Width <= 0 || item.Height <= 0 {
			continue
		}
		itemURL := strings.TrimSpace(item.URL)
		if itemURL == "" || !IsDownloadURLAllowed(itemURL) {
			continue
		}
		if _, seen := seenURLs[itemURL]; seen {
			continue
		}
		seenURLs[itemURL] = struct{}{}
		candidates = append(candidates, ImageAsset{
			URL:    itemURL,
			Width:  item.Width,
			Height: item.Height,
		})
	}
	if len(candidates) > maxPostRefGIFCandidates {
		candidates = candidates[:maxPostRefGIFCandidates]
	}
	return candidates
}

type PostRefBlog struct {
	UUID        string `json:"uuid,omitempty"`
	Name        string `json:"name,omitempty"`
	URL         string `json:"url,omitempty"`
	BlogViewURL string `json:"blogViewUrl,omitempty"`
	IsAdult     bool   `json:"isAdult,omitempty"`
	ShouldBlur  bool   `json:"shouldBlur,omitempty"`
}

func (b *PostRefBlog) UnmarshalJSON(data []byte) error {
	type postRefBlogAlias PostRefBlog
	var alias postRefBlogAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	*b = PostRefBlog(alias)
	var extra struct {
		BlogViewURL string `json:"blog_view_url"`
	}
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	if strings.TrimSpace(extra.BlogViewURL) != "" {
		b.BlogViewURL = extra.BlogViewURL
	}
	return nil
}

func decodePostRefBlog(data json.RawMessage) (PostRefBlog, error) {
	var blog PostRefBlog
	if err := json.Unmarshal(data, &blog); err == nil {
		blog.UUID = strings.TrimSpace(blog.UUID)
		blog.Name = strings.TrimSpace(blog.Name)
		blog.URL = strings.TrimSpace(blog.URL)
		blog.BlogViewURL = strings.TrimSpace(blog.BlogViewURL)
		return blog, nil
	}
	value := decodeFlexibleString(data)
	if value == "" {
		return PostRefBlog{}, nil
	}
	if name := NormalizeBlogName(value); name != "" {
		return PostRefBlog{Name: name}, nil
	}
	return PostRefBlog{UUID: value}, nil
}

type PostShare struct {
	ID   string `json:"id"`
	Blog string `json:"blog"`
	Type string `json:"type,omitempty"`
}

type SendMessageRequest struct {
	ConversationID string   `json:"conversation_id,omitempty"`
	Participants   []string `json:"participants,omitempty"`
	Type           string   `json:"type"`
	Context        string   `json:"context,omitempty"`
	Participant    string   `json:"participant"`
	StickerID      string   `json:"stickerId,omitempty"`
	Post           any      `json:"post,omitempty"`
	Message        string   `json:"message"`
}

type SendMessageResponse struct {
	Conversation *Conversation `json:"conversation"`
}

func (r *SendMessageResponse) UnmarshalJSON(data []byte) error {
	var wrapped struct {
		Conversation *Conversation `json:"conversation"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && wrapped.Conversation != nil {
		r.Conversation = wrapped.Conversation
		return nil
	}
	var conversation Conversation
	if err := json.Unmarshal(data, &conversation); err != nil {
		return err
	}
	r.Conversation = &conversation
	return nil
}
