package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

var _ bridgev2.IdentifierResolvingNetworkAPI = (*TumblrClient)(nil)
var _ bridgev2.UserSearchingNetworkAPI = (*TumblrClient)(nil)

func (tc *TumblrClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	normalized := tumblr.NormalizeBlogName(identifier)
	if normalized == "" {
		return nil, errors.New("blog name is empty")
	}
	if !tc.IsLoggedIn() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	if _, err := tc.validatedLoginMetadata(); err != nil {
		return nil, err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.GetBlogInfo(ctx, normalized)
	if err != nil {
		if tumblr.IsNotFound(err) {
			return nil, nil
		}
		return nil, tc.handleRemoteError(err)
	}
	if resp == nil || resp.Blog == nil || tumblrBlogUserID(*resp.Blog) == "" {
		return nil, nil
	}
	resolveResp, err := tc.resolveResponseForBlog(ctx, *resp.Blog)
	if err != nil || resolveResp == nil || !createChat {
		return resolveResp, err
	}
	chat, err := tc.createDMResponseForBlog(ctx, normalized, *resp.Blog)
	if err != nil {
		return nil, err
	}
	resolveResp.Chat = chat
	return resolveResp, nil
}

func (tc *TumblrClient) SearchUsers(ctx context.Context, query string) ([]*bridgev2.ResolveIdentifierResponse, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil
	}
	if !tc.IsLoggedIn() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return nil, err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.SearchParticipantSuggestions(ctx, meta.SelectedBlogName, query, 10)
	if err != nil {
		return nil, tc.handleRemoteError(err)
	}
	results := make([]*bridgev2.ResolveIdentifierResponse, 0, len(resp.Blogs))
	for _, blog := range resp.Blogs {
		blog, err = tc.hydrateSuggestionBlog(ctx, client, blog)
		if err != nil {
			return nil, err
		}
		resolveResp, err := tc.resolveResponseForBlog(ctx, blog)
		if err != nil {
			return nil, err
		}
		if resolveResp != nil {
			results = append(results, resolveResp)
		}
	}
	return results, nil
}

func (tc *TumblrClient) hydrateSuggestionBlog(ctx context.Context, client *tumblr.Client, blog tumblr.Blog) (tumblr.Blog, error) {
	if validRemoteID(blog.UUID) || tumblrBlogNameID(blog.Name) == "" || client == nil {
		return blog, nil
	}
	resp, err := client.GetBlogInfo(ctx, blog.Name)
	if err != nil {
		if tumblr.IsNotFound(err) {
			return tumblr.Blog{}, nil
		}
		return tumblr.Blog{}, tc.handleRemoteError(err)
	}
	if resp == nil || resp.Blog == nil || !validRemoteID(resp.Blog.UUID) {
		return tumblr.Blog{}, nil
	}
	if resp.Blog.Name == "" {
		resp.Blog.Name = blog.Name
	}
	if resp.Blog.Title == "" {
		resp.Blog.Title = blog.Title
	}
	if len(resp.Blog.Avatar) == 0 {
		resp.Blog.Avatar = blog.Avatar
	}
	return *resp.Blog, nil
}

func (tc *TumblrClient) createDMResponseForBlog(ctx context.Context, targetBlogName string, blog tumblr.Blog) (*bridgev2.CreateChatResponse, error) {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return nil, err
	}
	targetUserID := tumblrBlogUserID(blog)
	targetParticipantID := strings.TrimSpace(blog.UUID)
	if !validRemoteID(targetParticipantID) {
		return nil, bridgev2.RespError(mautrix.MForbidden.WithMessage(
			"Tumblr did not return the UUID needed to start this DM",
		))
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	if existing, err := client.GetConversationByParticipants(ctx, meta.SelectedBlogName, targetBlogName, 1); err == nil {
		if existing != nil && existing.Conversation != nil && validRemoteID(existing.Conversation.ID) {
			return &bridgev2.CreateChatResponse{
				PortalKey:  tc.portalKey(existing.Conversation.ID),
				PortalInfo: tc.chatInfoFromConversation(*existing.Conversation),
			}, nil
		}
	} else if !tumblr.IsNotFound(err) {
		return nil, tc.handleRemoteError(err)
	}
	participantIDs := []string{targetParticipantID, meta.SelectedBlogUUID}
	return &bridgev2.CreateChatResponse{
		PortalKey:  tc.pendingDMPortalKey(participantIDs),
		PortalInfo: tc.pendingDMChatInfo(blog, participantIDs, targetBlogName, targetUserID),
	}, nil
}

func (tc *TumblrClient) resolveResponseForBlog(ctx context.Context, blog tumblr.Blog) (*bridgev2.ResolveIdentifierResponse, error) {
	userID := tumblrBlogUserID(blog)
	if userID == "" {
		return nil, nil
	}
	var ghost *bridgev2.Ghost
	if tc.connector != nil && tc.connector.Bridge != nil {
		var err error
		ghost, err = tc.connector.Bridge.GetGhostByID(ctx, userID)
		if err != nil {
			return nil, err
		}
	}
	return &bridgev2.ResolveIdentifierResponse{
		Ghost:    ghost,
		UserID:   userID,
		UserInfo: tc.blogUserInfo(blog),
	}, nil
}

func (tc *TumblrClient) pendingDMPortalKey(participantIDs []string) networkid.PortalKey {
	joined := strings.Join(participantIDs, "\x00")
	sum := sha256.Sum256([]byte(joined))
	return tc.portalKey(pendingDMPortalPrefix + hex.EncodeToString(sum[:16]))
}

const pendingDMPortalPrefix = "pending-dm:"

func isPendingDMPortalID(portalID string) bool {
	return strings.HasPrefix(portalID, pendingDMPortalPrefix)
}

func (tc *TumblrClient) pendingDMChatInfo(blog tumblr.Blog, participantIDs []string, targetBlogName string, targetUserID networkid.UserID) *bridgev2.ChatInfo {
	conversation := tumblr.Conversation{
		ID: string(tc.pendingDMPortalKey(participantIDs).ID),
		Participants: []tumblr.Blog{
			{
				UUID: participantIDs[1],
				Name: tc.selectedBlogName(),
			},
			blog,
		},
	}
	info := tc.chatInfoFromConversation(conversation)
	info.ExtraUpdates = func(_ context.Context, portal *bridgev2.Portal) bool {
		if portal == nil || portal.Portal == nil {
			return false
		}
		portal.Metadata = &PortalMetadata{
			PendingParticipantIDs:  append([]string(nil), participantIDs...),
			PendingParticipantName: targetBlogName,
		}
		if targetUserID != "" {
			portal.OtherUserID = targetUserID
		}
		return true
	}
	return info
}

func (tc *TumblrClient) selectedBlogName() string {
	if meta, err := tc.validatedLoginMetadata(); err == nil {
		return meta.SelectedBlogName
	}
	return ""
}

func tumblrBlogUserID(blog tumblr.Blog) networkid.UserID {
	if validRemoteID(blog.UUID) {
		return tumblrid.MakeUserID(blog.UUID)
	}
	if name := tumblrBlogNameID(blog.Name); name != "" {
		return tumblrid.MakeUserID(name)
	}
	return ""
}

func tumblrBlogNameID(name string) string {
	raw := strings.TrimSpace(name)
	if raw == "" {
		return ""
	}
	normalized := tumblr.NormalizeBlogName(raw)
	if normalized == "" || !validRemoteID(normalized) {
		return ""
	}
	canonicalRaw := strings.TrimPrefix(strings.ToLower(raw), "@")
	if canonicalRaw != normalized {
		return ""
	}
	return normalized
}

func (tc *TumblrClient) blogUserInfo(blog tumblr.Blog) *bridgev2.UserInfo {
	displayName := tc.blogDisplayName(blog)
	info := &bridgev2.UserInfo{}
	if displayName != "" {
		info.Name = &displayName
	}
	if name := tumblrBlogNameID(blog.Name); name != "" {
		info.Identifiers = []string{"tumblr:" + name}
	}
	info.Avatar = tc.blogAvatar(blog)
	return info
}
