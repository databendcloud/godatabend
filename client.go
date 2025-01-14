package godatabend

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type AuthMethod string

const (
	AuthMethodUserPassword AuthMethod = "userPassword"
	AuthMethodAccessToken  AuthMethod = "accessToken"
)

type RequestType int

// request type
const (
	Query RequestType = iota
	Page
	Final
	Kill
)

type ContextKey string

const (
	ContextKeyQueryID  ContextKey = "X-DATABEND-QUERY-ID"
	ContextUserAgentID ContextKey = "USER-AGENT"

	EMPTY_FIELD_AS string = "empty_field_as"
	PURGE          string = "purge"
)

type PresignedResponse struct {
	Method  string
	Headers map[string]string
	URL     string
}

type StageLocation struct {
	Name string
	Path string
}

func (sl *StageLocation) String() string {
	return fmt.Sprintf("@%s/%s", sl.Name, sl.Path)
}

func (c *APIClient) NewDefaultCSVFormatOptions() map[string]string {
	return map[string]string{
		"type":             "CSV",
		"field_delimiter":  ",",
		"record_delimiter": "\n",
		"skip_header":      "0",
		EMPTY_FIELD_AS:     c.EmptyFieldAs,
	}
}

func (c *APIClient) NewDefaultCopyOptions() map[string]string {
	return map[string]string{
		PURGE: "true",
	}
}

type APIClient struct {
	SessionID string
	QuerySeq  int64
	NodeID    string
	cli       *http.Client
	rows      *nextRows

	apiEndpoint string
	host        string
	tenant      string
	warehouse   string
	database    string
	user        string
	password    string

	sessionStateRaw *json.RawMessage
	sessionState    *SessionState

	// routHint is used to save the route hint from the last responded X-Databend-Route-Hint, this is
	// used for guiding the preferred route for the next following http requests, this is useful for
	// some cases like query pagination & multi-statements transaction.
	routeHint string

	statsTracker      QueryStatsTracker
	accessTokenLoader AccessTokenLoader

	WaitTimeSeconds      int64
	MaxRowsInBuffer      int64
	MaxRowsPerPage       int64
	PresignedURLDisabled bool
	EmptyFieldAs         string

	// only used for testing mocks
	doRequestFunc func(method, path string, req interface{}, resp interface{}) error
}

func (c *APIClient) NextQuery() {
	if c.rows != nil {
		_ = c.rows.Close()
	}
	c.QuerySeq += 1
}

func (c *APIClient) GetQueryID() string {
	return fmt.Sprintf("%s.%d", c.SessionID, c.QuerySeq)
}

func (c *APIClient) NeedSticky() bool {
	if c.sessionState != nil {
		return c.sessionState.NeedSticky
	}
	return false
}

func (c *APIClient) NeedKeepAlive() bool {
	if c.sessionState != nil {
		return c.sessionState.NeedKeepAlive
	}
	return false
}

func NewAPIHttpClientFromConfig(cfg *Config) *http.Client {
	jar := NewIgnoreDomainCookieJar()
	jar.SetCookies(nil, []*http.Cookie{{Name: "cookie_enabled", Value: "true"}})
	cli := &http.Client{
		Timeout: cfg.Timeout,
		Jar:     jar,
	}
	if cfg.EnableOpenTelemetry {
		cli.Transport = otelhttp.NewTransport(http.DefaultTransport)
	}
	return cli
}

func NewAPIClientFromConfig(cfg *Config) *APIClient {
	var apiScheme string
	switch cfg.SSLMode {
	case SSL_MODE_DISABLE:
		apiScheme = "http"
	default:
		apiScheme = "https"
	}

	// if role is set in config, we'd prefer to limit it as the only effective role,
	// so you could limit the privileges by setting a role with limited privileges.
	// however, this can be overridden by executing `SET SECONDARY ROLES ALL` in the
	// query.
	// secondaryRoles now have two viable values:
	// - nil: means enabling ALL the granted roles of the user
	// - []string{}: means enabling NONE of the granted roles
	var secondaryRoles *[]string
	if len(cfg.Role) > 0 {
		secondaryRoles = &[]string{}
	}

	var sessionState = SessionState{
		Database:       cfg.Database,
		Role:           cfg.Role,
		SecondaryRoles: secondaryRoles,
		Settings:       cfg.Params,
	}
	sessionStateRawJson, _ := json.Marshal(sessionState)
	sessionStateRaw := json.RawMessage(sessionStateRawJson)

	return &APIClient{
		SessionID:       uuid.NewString(),
		cli:             NewAPIHttpClientFromConfig(cfg),
		apiEndpoint:     fmt.Sprintf("%s://%s", apiScheme, cfg.Host),
		host:            cfg.Host,
		tenant:          cfg.Tenant,
		warehouse:       cfg.Warehouse,
		user:            cfg.User,
		password:        cfg.Password,
		sessionState:    &sessionState,
		sessionStateRaw: &sessionStateRaw,
		routeHint:       randRouteHint(),

		accessTokenLoader: initAccessTokenLoader(cfg),
		statsTracker:      cfg.StatsTracker,

		WaitTimeSeconds:      cfg.WaitTimeSecs,
		MaxRowsInBuffer:      cfg.MaxRowsInBuffer,
		MaxRowsPerPage:       cfg.MaxRowsPerPage,
		PresignedURLDisabled: cfg.PresignedURLDisabled,
		EmptyFieldAs:         cfg.EmptyFieldAs,
	}
}

func initAccessTokenLoader(cfg *Config) AccessTokenLoader {
	if cfg.AccessTokenLoader != nil {
		return cfg.AccessTokenLoader
	} else if cfg.AccessTokenFile != "" {
		return NewFileAccessTokenLoader(cfg.AccessTokenFile)
	} else if cfg.AccessToken != "" {
		return NewStaticAccessTokenLoader(cfg.AccessToken)
	}
	return nil
}

func (c *APIClient) doRequest(ctx context.Context, method, path string, req interface{}, needSticky bool, resp interface{}, respHeaders *http.Header) error {
	if c.doRequestFunc != nil {
		return c.doRequestFunc(method, path, req, resp)
	}

	var err error
	reqBody := []byte{}
	if req != nil {
		reqBody, err = json.Marshal(req)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	url := c.makeURL(path)
	httpReq, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq = httpReq.WithContext(ctx)

	maxRetries := 2
	for i := 1; i <= maxRetries; i++ {
		// do not retry if context is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		headers, err := c.makeHeaders(ctx)
		if err != nil {
			return fmt.Errorf("failed to make request headers: %w", err)
		}
		if needSticky && len(c.NodeID) != 0 {
			headers.Set(DatabendQueryStickyNode, c.NodeID)
		}
		if method == "GET" && len(c.NodeID) != 0 {
			headers.Set(DatabendQueryIDNode, c.NodeID)
		}
		headers.Set(contentType, jsonContentType)
		headers.Set(accept, jsonContentType)
		httpReq.Header = headers
		if len(c.host) > 0 {
			httpReq.Host = c.host
		}

		httpResp, err := c.cli.Do(httpReq)
		if err != nil {
			return errors.Join(ErrDoRequest, err)
		}
		defer func() {
			_ = httpResp.Body.Close()
		}()

		httpRespBody, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return errors.Join(ErrReadResponse, err)
		}

		if httpResp.StatusCode == http.StatusUnauthorized {
			if c.authMethod() == AuthMethodAccessToken && i < maxRetries {
				// retry with a rotated access token
				_, _ = c.accessTokenLoader.LoadAccessToken(context.Background(), true)
				continue
			}
			return NewAPIError("authorization failed", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode >= 500 {
			return NewAPIError("please retry again later", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode >= 400 {
			return NewAPIError("please check your arguments", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode != 200 {
			return NewAPIError("unexpected HTTP StatusCode", httpResp.StatusCode, httpRespBody)
		}

		if resp != nil {
			contentType := httpResp.Header.Get("Content-Type")
			if strings.HasPrefix(contentType, "application/json") {
				if err := json.Unmarshal(httpRespBody, &resp); err != nil {
					return fmt.Errorf("failed to unmarshal response body: %w", err)
				}
			}
		}
		if respHeaders != nil {
			*respHeaders = httpResp.Header
		}
		return nil
	}
	return fmt.Errorf("failed to do request after %d retries", maxRetries)
}

func (c *APIClient) trackStats(resp *QueryResponse) {
	if c.statsTracker == nil || resp == nil || resp.Stats == nil {
		return
	}
	c.statsTracker(resp.ID, resp.Stats)
}

func (c *APIClient) makeURL(path string, args ...interface{}) string {
	format := c.apiEndpoint + path
	return fmt.Sprintf(format, args...)
}

func (c *APIClient) authMethod() AuthMethod {
	if c.user != "" {
		return AuthMethodUserPassword
	}
	if c.accessTokenLoader != nil {
		return AuthMethodAccessToken
	}
	return ""
}

func (c *APIClient) makeHeaders(ctx context.Context) (http.Header, error) {
	headers := http.Header{}
	headers.Set(WarehouseRoute, "warehouse")
	headers.Set(UserAgent, fmt.Sprintf("databend-go/%s", version))
	if userAgent, ok := ctx.Value(ContextUserAgentID).(string); ok {
		headers.Set(UserAgent, fmt.Sprintf("databend-go/%s/%s", version, userAgent))
	}
	if c.tenant != "" {
		headers.Set(DatabendTenantHeader, c.tenant)
	}
	if c.warehouse != "" {
		headers.Set(DatabendWarehouseHeader, c.warehouse)
	}
	if c.routeHint != "" {
		headers.Set(DatabendRouteHintHeader, c.routeHint)
	}

	if queryID, ok := ctx.Value(ContextKeyQueryID).(string); ok {
		headers.Set(DatabendQueryIDHeader, queryID)
	} else {
		headers.Set(DatabendQueryIDHeader, c.GetQueryID())
	}

	switch c.authMethod() {
	case AuthMethodUserPassword:
		headers.Set(Authorization, fmt.Sprintf("Basic %s", encode(c.user, c.password)))
	case AuthMethodAccessToken:
		accessToken, err := c.accessTokenLoader.LoadAccessToken(context.TODO(), false)
		if err != nil {
			return nil, fmt.Errorf("failed to load access token: %w", err)
		}
		headers.Set(Authorization, fmt.Sprintf("Bearer %s", accessToken))
	default:
		return nil, fmt.Errorf("no user password or access token")
	}

	return headers, nil
}

func encode(name string, key string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", name, key)))
}

// databendInsecureTransport is the transport object that doesn't do certificate revocation check.
var databendInsecureTransport = &http.Transport{
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
	Proxy:           http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
}

func (c *APIClient) getPagenationConfig() *PaginationConfig {
	if c.MaxRowsPerPage == 0 && c.MaxRowsInBuffer == 0 && c.WaitTimeSeconds == 0 {
		return nil
	}
	return &PaginationConfig{
		MaxRowsPerPage:  c.MaxRowsPerPage,
		MaxRowsInBuffer: c.MaxRowsInBuffer,
		WaitTime:        c.WaitTimeSeconds,
	}
}

func (c *APIClient) getSessionStateRaw() *json.RawMessage {
	return c.sessionStateRaw
}

func (c *APIClient) getSessionState() *SessionState {
	return c.sessionState
}

func (c *APIClient) inActiveTransaction() bool {
	return c.sessionState != nil && strings.EqualFold(string(c.sessionState.TxnState), string(TxnStateActive))
}

func (c *APIClient) applySessionState(response *QueryResponse) {
	if response == nil || response.Session == nil {
		return
	}
	c.sessionStateRaw = response.Session
	_ = json.Unmarshal(*response.Session, c.sessionState)
}

func (c *APIClient) PollUntilQueryEnd(ctx context.Context, resp *QueryResponse) (*QueryResponse, error) {
	var err error
	for !resp.ReadFinished() {
		data := resp.Data
		resp, err = c.PollQuery(ctx, resp.NextURI)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// context might be canceled due to timeout or canceled. if it's canceled, we need call
				// the kill url to tell the backend it's killed.
				fmt.Printf("query canceled, kill query:%s", resp.ID)
				_ = c.KillQuery(context.Background(), resp)
			}
			return nil, err
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("query page has error: %w", resp.Error)
		}
		resp.Data = append(data, resp.Data...)
	}
	return resp, nil
}

func buildQuery(query string, params []driver.Value) (string, error) {
	if len(params) > 0 && params[0] != nil {
		result, err := interpolateParams(query, params)
		if err != nil {
			return result, fmt.Errorf("buildRequest: failed to interpolate params: %w", err)
		}
		return result, nil
	}
	return query, nil
}

func (c *APIClient) QuerySync(ctx context.Context, query string, args []driver.Value) (*QueryResponse, error) {
	resp, err := c.StartQuery(ctx, query, args)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = c.CloseQuery(ctx, resp)
	}()
	if resp.Error != nil {
		return nil, fmt.Errorf("query error: %+v", resp.Error)
	}
	return c.PollUntilQueryEnd(ctx, resp)
}

func (c *APIClient) doRetry(f retry.RetryableFunc, t RequestType) error {
	var delay time.Duration = 1
	var attempts uint = 3
	if t == Query {
		delay = 2
		attempts = 5
	}
	return retry.Do(
		func() error {
			return f()
		},
		retry.RetryIf(func(err error) bool {
			if err == nil {
				return false
			}
			if errors.Is(err, context.Canceled) {
				return false
			}
			if errors.Is(err, ErrDoRequest) || errors.Is(err, ErrReadResponse) || IsProxyErr(err) {
				return true
			}
			if t == Query && strings.Contains(err.Error(), ProvisionWarehouseTimeout) {
				return true
			}
			return false
		}),
		retry.Delay(delay*time.Second),
		retry.Attempts(attempts),
		retry.DelayType(retry.FixedDelay),
	)
}

func (c *APIClient) startQueryRequest(ctx context.Context, request *QueryRequest) (*QueryResponse, error) {
	c.NextQuery()
	// fmt.Printf("start query %v %v\n", c.GetQueryID(), request.SQL)

	if !c.inActiveTransaction() {
		c.routeHint = randRouteHint()
	}

	path := "/v1/query"
	var (
		resp        QueryResponse
		respHeaders http.Header
	)
	err := c.doRetry(func() error {
		return c.doRequest(ctx, "POST", path, request, c.NeedSticky(), &resp, &respHeaders)
	}, Query,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to do query request: %w", err)
	}

	if len(resp.NodeID) != 0 {
		c.NodeID = resp.NodeID
	}
	c.trackStats(&resp)
	// try update session as long as resp is not nil, even if query failed (resp.Error != nil)
	// e.g. transaction state need to be updated if commit fail
	c.applySessionState(&resp)
	// save route hint for the next following http requests
	if len(respHeaders) > 0 && len(respHeaders.Get(DatabendRouteHintHeader)) > 0 {
		c.routeHint = respHeaders.Get(DatabendRouteHintHeader)
	}
	return &resp, nil
}

func (c *APIClient) StartQuery(ctx context.Context, query string, args []driver.Value) (*QueryResponse, error) {
	q, err := buildQuery(query, args)
	if err != nil {
		return nil, err
	}
	request := QueryRequest{
		SQL:        q,
		Pagination: c.getPagenationConfig(),
		Session:    c.getSessionStateRaw(),
	}
	return c.startQueryRequest(ctx, &request)
}

func (c *APIClient) PollQuery(ctx context.Context, nextURI string) (*QueryResponse, error) {
	var result QueryResponse
	err := c.doRetry(
		func() error {
			return c.doRequest(ctx, "GET", nextURI, nil, true, &result, nil)
		},
		Page,
	)
	// try update session as long as resp is not nil, even if query failed (resp.Error != nil)
	// e.g. transaction state need to be updated if commit fail
	c.applySessionState(&result)
	c.trackStats(&result)
	if err != nil {
		return nil, fmt.Errorf("failed to query page: %w", err)
	}
	return &result, nil
}

func (c *APIClient) KillQuery(ctx context.Context, response *QueryResponse) error {
	if response != nil && response.KillURI != "" {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_ = c.doRetry(func() error {
			return c.doRequest(ctx, "GET", response.KillURI, nil, true, nil, nil)
		}, Kill,
		)
	}
	return nil
}

func (c *APIClient) CloseQuery(ctx context.Context, response *QueryResponse) error {
	if response != nil && response.FinalURI != "" {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_ = c.doRetry(func() error {
			return c.doRequest(ctx, "GET", response.FinalURI, nil, true, nil, nil)
		}, Final,
		)
	}
	return nil
}

func (c *APIClient) InsertWithStage(ctx context.Context, sql string, stage *StageLocation, fileFormatOptions, copyOptions map[string]string) (*QueryResponse, error) {
	if stage == nil {
		return nil, errors.New("stage location required for insert with stage")
	}
	if fileFormatOptions == nil {
		fileFormatOptions = c.NewDefaultCSVFormatOptions()
	}
	if copyOptions == nil {
		copyOptions = c.NewDefaultCopyOptions()
	}
	request := QueryRequest{
		SQL:        sql,
		Pagination: c.getPagenationConfig(),
		Session:    c.getSessionStateRaw(),
		StageAttachment: &StageAttachmentConfig{
			Location:          stage.String(),
			FileFormatOptions: fileFormatOptions,
			CopyOptions:       copyOptions,
		},
	}
	resp, err := c.startQueryRequest(ctx, &request)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = c.CloseQuery(ctx, resp)
	}()
	if resp.Error != nil {
		return nil, fmt.Errorf("query error: %w", resp.Error)
	}
	return c.PollUntilQueryEnd(ctx, resp)
}

func (c *APIClient) UploadToStage(ctx context.Context, stage *StageLocation, input *bufio.Reader, size int64) error {
	if c.PresignedURLDisabled {
		return c.UploadToStageByAPI(ctx, stage, input)
	} else {
		return c.UploadToStageByPresignURL(ctx, stage, input, size)
	}
}

func (c *APIClient) GetPresignedURL(ctx context.Context, stage *StageLocation) (*PresignedResponse, error) {
	presignUploadSQL := fmt.Sprintf("PRESIGN UPLOAD %s", stage)
	resp, err := c.QuerySync(ctx, presignUploadSQL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query presign url: %w", err)
	}
	if len(resp.Data) < 1 || len(resp.Data[0]) < 2 {
		return nil, fmt.Errorf("generate presign url invalid response: %+v", resp.Data)
	}
	if resp.Data[0][0] == nil || resp.Data[0][1] == nil || resp.Data[0][2] == nil {
		return nil, fmt.Errorf("generate presign url invalid response: %+v", resp.Data)
	}
	method := *resp.Data[0][0]
	url := *resp.Data[0][2]
	headers := map[string]string{}
	err = json.Unmarshal([]byte(*resp.Data[0][1]), &headers)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal headers: %w", err)
	}
	result := &PresignedResponse{
		Method:  method,
		Headers: headers,
		URL:     url,
	}
	return result, nil
}

func (c *APIClient) UploadToStageByPresignURL(ctx context.Context, stage *StageLocation, input *bufio.Reader, size int64) error {
	presigned, err := c.GetPresignedURL(ctx, stage)
	if err != nil {
		return fmt.Errorf("failed to get presigned url: %w", err)
	}

	req, err := http.NewRequest("PUT", presigned.URL, input)
	if err != nil {
		return err
	}
	for k, v := range presigned.Headers {
		req.Header.Set(k, v)
	}
	req.ContentLength = size
	// TODO: configurable timeout
	httpClient := &http.Client{
		Timeout: time.Second * 60,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload to stage by presigned url: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("failed to upload to stage by presigned url, status code: %d, body: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (c *APIClient) UploadToStageByAPI(ctx context.Context, stage *StageLocation, input *bufio.Reader) error {
	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("upload", stage.Path)
	if err != nil {
		return fmt.Errorf("failed to create multipart writer form file: %w", err)
	}
	// TODO: do async upload
	_, err = io.Copy(part, input)
	if err != nil {
		return fmt.Errorf("failed to copy file to multipart writer form file: %w", err)
	}
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close multipart writer: %w", err)
	}

	path := "/v1/upload_to_stage"
	url := c.makeURL(path)
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header, err = c.makeHeaders(ctx)
	if err != nil {
		return fmt.Errorf("failed to make headers: %w", err)
	}
	if len(c.host) > 0 {
		req.Host = c.host
	}
	req.Header.Set("stage_name", stage.Name)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// TODO: configurable timeout
	httpClient := &http.Client{
		Timeout: time.Second * 60,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed http do request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read http response body: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return NewAPIError("please check your user/password.", resp.StatusCode, respBody)
	} else if resp.StatusCode >= 500 {
		return NewAPIError("please retry again later.", resp.StatusCode, respBody)
	} else if resp.StatusCode >= 400 {
		return NewAPIError("please check your arguments.", resp.StatusCode, respBody)
	}

	return nil
}

func (c *APIClient) Logout(ctx context.Context) error {
	if c.NeedKeepAlive() {
		req := &struct{}{}
		return c.doRequest(ctx, "POST", "/v1/session/logout/", req, c.NeedSticky(), nil, nil)
	}
	return nil
}

func randRouteHint() string {
	charset := "abcdef0123456789"
	b := make([]byte, 16)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
