package tumblr

import (
	"context"
	"net/http"
)

type WebPushKeys struct {
	P256DH string `json:"p256dh"`
	Auth   string `json:"auth"`
}

type WebPushDeviceRegistration struct {
	WebEndpoint       string      `json:"web_endpoint"`
	WebExpirationTime *int64      `json:"web_expiration_time"`
	WebKeys           WebPushKeys `json:"web_keys"`
	UUID              string      `json:"uuid"`
	ServiceType       string      `json:"service_type"`
	AppID             int         `json:"app_id"`
}

func NewWebPushDeviceRegistration(endpoint string, keys WebPushKeys) WebPushDeviceRegistration {
	return WebPushDeviceRegistration{
		WebEndpoint: endpoint,
		WebKeys:     keys,
		UUID:        webPushUUID(endpoint),
		ServiceType: "web",
		AppID:       1,
	}
}

func webPushUUID(endpoint string) string {
	if len(endpoint) <= 256 {
		return endpoint
	}
	return endpoint[len(endpoint)-256:]
}

func (c *Client) RegisterWebPushDevice(ctx context.Context, registration WebPushDeviceRegistration) error {
	return c.do(ctx, http.MethodPost, "/v2/device/register", nil, registration, nil)
}

func (c *Client) UnregisterWebPushDevice(ctx context.Context, registration WebPushDeviceRegistration) error {
	return c.do(ctx, http.MethodPost, "/v2/device/unregister", nil, registration, nil)
}
