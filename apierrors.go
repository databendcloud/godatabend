// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package godatabend

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ProvisionWarehouseTimeout = "ProvisionWarehouseTimeout"

	ErrDoRequest    = errors.New("failed to do request")
	ErrReadResponse = errors.New("failed to read response")
)

type APIErrorResponseBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type APIError struct {
	RespBody   APIErrorResponseBody
	RespText   string
	StatusCode int
	Hint       string
}

func (e APIError) Error() string {
	message := e.RespBody.Message
	if message == "" {
		message = e.RespText
	}
	message = fmt.Sprintf("%d %s", e.StatusCode, message)
	if e.Hint != "" {
		message = strings.Trim(message, ".")
		message += ". " + e.Hint
	}
	return message
}

func NewAPIError(hint string, status int, respBuf []byte) error {
	respBody := APIErrorResponseBody{}
	_ = json.Unmarshal(respBuf, &respBody)
	return APIError{
		RespBody:   respBody,
		RespText:   string(respBuf),
		StatusCode: status,
		Hint:       hint,
	}
}

func IsNotFound(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 404
}

func IsProxyErr(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 520
}

func IsAuthFailed(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 401
}

func RespBody(err error) APIErrorResponseBody {
	var apiErr APIError
	if !errors.As(err, &apiErr) {
		return APIErrorResponseBody{}
	}
	return apiErr.RespBody
}
