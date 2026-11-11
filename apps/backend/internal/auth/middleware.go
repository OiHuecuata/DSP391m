package auth

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Require verifies the opaque session with the frontend account service.
// Direct calls to the Go port must obey the same permissions as the website.
func Require(admin bool) gin.HandlerFunc {
	client := &http.Client{Timeout: 35 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	endpoint := os.Getenv("AUTH_SERVICE_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:3000/api/auth/me"
	}
	return func(c *gin.Context) {
		token, err := c.Cookie("pickleballdb_session")
		if err != nil || len(token) != 64 {
			c.AbortWithStatusJSON(401, gin.H{"error": "Please sign in."})
			return
		}
		if c.Request.Method != "GET" && c.Request.Method != "HEAD" {
			if origin := c.GetHeader("Origin"); origin != "" {
				if !allowedOrigin(origin, endpoint) {
					c.AbortWithStatusJSON(403, gin.H{"error": "Invalid request origin."})
					return
				}
			}
			if strings.EqualFold(c.GetHeader("Sec-Fetch-Site"), "cross-site") {
				c.AbortWithStatusJSON(403, gin.H{"error": "Invalid request origin."})
				return
			}
		}
		req, err := http.NewRequestWithContext(c.Request.Context(), "GET", endpoint, nil)
		if err != nil {
			c.AbortWithStatus(503)
			return
		}
		req.AddCookie(&http.Cookie{Name: "pickleballdb_session", Value: token})
		result, err := client.Do(req)
		if err != nil {
			c.AbortWithStatusJSON(503, gin.H{"error": "Account service unavailable."})
			return
		}
		defer result.Body.Close()
		if result.StatusCode != 200 {
			status := 401
			if result.StatusCode >= 500 {
				status = 503
			}
			c.AbortWithStatusJSON(status, gin.H{"error": "Sign-in verification failed."})
			return
		}
		var body struct {
			User *struct {
				Role       string `json:"role"`
				MustChange bool   `json:"mustChangePassword"`
				Active     bool   `json:"active"`
			} `json:"user"`
		}
		if json.NewDecoder(http.MaxBytesReader(c.Writer, result.Body, 65536)).Decode(&body) != nil || body.User == nil || !body.User.Active {
			c.AbortWithStatus(401)
			return
		}
		if body.User.MustChange {
			c.AbortWithStatusJSON(428, gin.H{"error": "Change your temporary password first."})
			return
		}
		if admin && body.User.Role != "admin" {
			c.AbortWithStatusJSON(403, gin.H{"error": "Administrator access required."})
			return
		}
		c.Next()
	}
}

// Cloudflare demo requests use one explicit HTTPS origin; local access remains supported.
func allowedOrigin(origin, accountService string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if local {
		port := u.Port()
		if port == "3000" || port == "8080" || (os.Getenv("SERVER_PORT") != "" && port == os.Getenv("SERVER_PORT")) {
			return true
		}
		account, e := url.Parse(accountService)
		return e == nil && port != "" && port == account.Port() && (account.Hostname() == "localhost" || account.Hostname() == "127.0.0.1" || account.Hostname() == "::1")
	}
	configured := os.Getenv("TRACKER_PUBLIC_ORIGIN")
	allowed, err := url.Parse(configured)
	return configured != "" && err == nil && allowed.Scheme == "https" && allowed.User == nil && allowed.RawQuery == "" && allowed.Fragment == "" && (allowed.Path == "" || allowed.Path == "/") && u.Scheme == "https" && u.Host == allowed.Host
}
