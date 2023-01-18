package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/pkg/errors"
)

type APIClient struct {
	cli *http.Client

	ApiEndpoint string
	Host        string

	User     string
	Password string

	AccessToken string

	Tenant    string
	Warehouse string

	PresignedURLDisabled bool
}

func NewAPIClientFromConfig(cfg *Config) *APIClient {
	var apiScheme string
	switch cfg.SSLMode {
	case SSL_MODE_DISABLE:
		apiScheme = "http"
	default:
		apiScheme = "https"
	}
	return &APIClient{
		cli: &http.Client{
			Timeout: cfg.Timeout,
		},
		ApiEndpoint: fmt.Sprintf("%s://%s", apiScheme, cfg.Host),
		Host:        cfg.Host,
		Tenant:      cfg.Tenant,
		Warehouse:   cfg.Warehouse,
		User:        cfg.User,
		Password:    cfg.Password,
		AccessToken: cfg.AccessToken,

		PresignedURLDisabled: cfg.PresignedURLDisabled,
	}
}

func (c *APIClient) doRequest(path string, req interface{}, resp interface{}) error {
	var err error
	reqBody := []byte{}
	if req != nil {
		reqBody, err = json.Marshal(req)
		if err != nil {
			return errors.Wrap(err, "failed to marshal request body")
		}
	}

	url := c.makeURL(path)
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return errors.Wrap(err, "failed to create http request")
	}

	headers, err := c.makeHeaders()
	if err != nil {
		return errors.Wrap(err, "failed to make request headers")
	}
	headers.Set(contentType, jsonContentType)
	headers.Set(accept, jsonContentType)
	httpReq.Header = headers

	if len(c.Host) > 0 {
		httpReq.Host = c.Host
	}

	httpResp, err := c.cli.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed http do request: %w", err)
	}
	defer httpResp.Body.Close()

	httpRespBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("io read error: %w", err)
	}

	if httpResp.StatusCode == http.StatusUnauthorized {
		return NewAPIError("please check your user/password.", httpResp.StatusCode, httpRespBody)
	} else if httpResp.StatusCode >= 500 {
		return NewAPIError("please retry again later.", httpResp.StatusCode, httpRespBody)
	} else if httpResp.StatusCode >= 400 {
		return NewAPIError("please check your arguments.", httpResp.StatusCode, httpRespBody)
	}

	if resp != nil {
		if err := json.Unmarshal(httpRespBody, &resp); err != nil {
			return err
		}
	}
	return nil
}
func (c *APIClient) makeURL(path string, args ...interface{}) string {
	format := c.ApiEndpoint + path
	return fmt.Sprintf(format, args...)
}

func (c *APIClient) makeHeaders() (http.Header, error) {
	headers := http.Header{}
	headers.Set(WarehouseRoute, "warehouse")
	if c.Tenant != "" {
		headers.Set(DatabendTenantHeader, c.Tenant)
	}
	if c.Warehouse != "" {
		headers.Set(DatabendWarehouseHeader, c.Warehouse)
	}

	if c.User != "" {
		headers.Set(Authorization, fmt.Sprintf("Basic %s", encode(c.User, c.Password)))
	} else if c.AccessToken != "" {
		headers.Set(Authorization, fmt.Sprintf("Bearer %s", c.AccessToken))
	} else {
		return nil, errors.New("no user or access token")
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

func (c *APIClient) DoQuery(ctx context.Context, query string, args []driver.Value) (*QueryResponse, error) {
	q, err := buildQuery(query, args)
	if err != nil {
		return nil, err
	}
	request := QueryRequest{
		SQL: q,
		Pagination: Pagination{
			WaitTime: 90,
		},
	}
	path := "/v1/query"
	var result QueryResponse
	err = c.doRequest(path, request, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
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

func (c *APIClient) QuerySync(ctx context.Context, query string, args []driver.Value, respCh chan QueryResponse) error {
	// fmt.Printf("query sync %s", query)
	var r0 *QueryResponse
	err := retry.Do(
		func() error {
			r, err := c.DoQuery(ctx, query, args)
			if err != nil {
				return err
			}
			r0 = r
			return nil
		},
		// other err no need to retry
		retry.RetryIf(func(err error) bool {
			if err != nil && (IsProxyErr(err) || strings.Contains(err.Error(), ProvisionWarehouseTimeout)) {
				return true
			}
			return false
		}),
		retry.Delay(2*time.Second),
		retry.Attempts(10),
	)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	if r0.Error != nil {
		return fmt.Errorf("query has error: %+v", r0.Error)
	}
	if err != nil {
		return err
	}
	respCh <- *r0
	nextUri := r0.NextURI
	for len(nextUri) != 0 {
		p, err := c.QueryPage(nextUri)
		if err != nil {
			return err
		}
		if p.Error != nil {
			return fmt.Errorf("query has error: %+v", p.Error)
		}
		nextUri = p.NextURI
		respCh <- *p
	}
	return nil
}

func (c *APIClient) QueryPage(nextURI string) (*QueryResponse, error) {
	var result QueryResponse
	err := c.doRequest(nextURI, nil, &result)
	if err != nil {
		return nil, fmt.Errorf("query page failed: %w", err)
	}
	return &result, nil
}

func (c *APIClient) uploadToStage(fileName string) error {
	rootStage := "~"
	if c.PresignedURLDisabled {
		return c.uploadToStageByAPI(rootStage, fileName)
	} else {
		return c.UploadToStageByPresignURL(rootStage, fileName)
	}
}

func (c *APIClient) UploadToStageByPresignURL(stage, fileName string) error {
	presignUploadSQL := fmt.Sprintf("PRESIGN UPLOAD @%s/%s", stage, filepath.Base(fileName))
	resp, err := c.DoQuery(context.Background(), presignUploadSQL, nil)
	if err != nil {
		return err
	}
	if len(resp.Data) < 1 || len(resp.Data[0]) < 2 {
		return fmt.Errorf("generate presign url failed")
	}
	headers := make(map[string]string)
	err = json.Unmarshal([]byte(resp.Data[0][1]), &headers)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal presign url headers")
	}

	presignURL := fmt.Sprintf("%v", resp.Data[0][2])

	fileContent, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	body := bytes.NewBuffer(fileContent)

	httpReq, err := http.NewRequest("PUT", presignURL, body)
	if err != nil {
		return err
	}
	for k, v := range headers {
		httpReq.Header.Set(k, fmt.Sprintf("%v", v))
	}
	httpReq.Header.Set("Content-Length", strconv.FormatInt(int64(len(body.Bytes())), 10))
	httpClient := &http.Client{
		Timeout: time.Second * 60,
	}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed http do request: %w", err)
	}
	defer httpResp.Body.Close()
	httpRespBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("request got bad status: %d req=%s resp=%s", httpResp.StatusCode, body, httpRespBody)
	}
	return nil
}

func (c *APIClient) uploadToStageByAPI(stage, fileName string) error {
	body := new(bytes.Buffer)

	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("upload", file.Name())
	if err != nil {
		return errors.Wrap(err, "failed to create multipart writer form file")
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return errors.Wrap(err, "failed to copy file to multipart writer form file")
	}
	err = writer.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close multipart writer")
	}

	path := "/v1/upload_to_stage"
	url := c.makeURL(path)
	httpReq, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return errors.Wrap(err, "failed to create http request")
	}

	httpReq.Header, err = c.makeHeaders()
	if err != nil {
		return errors.Wrap(err, "failed to make headers")
	}
	if len(c.Host) > 0 {
		httpReq.Host = c.Host
	}
	httpReq.Header.Set("stage_name", stage)
	httpReq.Header.Set("Content-Type", writer.FormDataContentType())

	httpClient := &http.Client{
		Timeout: time.Second * 60,
	}
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return errors.Wrap(err, "failed http do request")
	}
	defer httpResp.Body.Close()

	httpRespBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read http response body")
	}

	if httpResp.StatusCode == http.StatusUnauthorized {
		return NewAPIError("please check your user/password.", httpResp.StatusCode, httpRespBody)
	} else if httpResp.StatusCode >= 500 {
		return NewAPIError("please retry again later.", httpResp.StatusCode, httpRespBody)
	} else if httpResp.StatusCode >= 400 {
		return NewAPIError("please check your arguments.", httpResp.StatusCode, httpRespBody)
	}

	return nil
}
