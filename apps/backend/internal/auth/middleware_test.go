package auth

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDirectBackendAuthorization(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, body    string
		admin, cookie bool
		want          int
	}{{"guest", "", true, false, 401}, {"user denied", `{"user":{"role":"user","active":true}}`, true, true, 403}, {"user read", `{"user":{"role":"user","active":true}}`, false, true, 200}, {"admin allowed", `{"user":{"role":"admin","active":true}}`, true, true, 200}, {"temporary password", `{"user":{"role":"admin","active":true,"mustChangePassword":true}}`, true, true, 428}, {"disabled", `{"user":{"role":"admin","active":false}}`, true, true, 401}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			t.Setenv("AUTH_SERVICE_URL", server.URL)
			router := gin.New()
			router.GET("/protected", Require(tc.admin), func(c *gin.Context) { c.Status(200) })
			req := httptest.NewRequest("GET", "/protected", nil)
			if tc.cookie {
				req.AddCookie(&http.Cookie{Name: "pickleballdb_session", Value: strings.Repeat("a", 64)})
			}
			result := httptest.NewRecorder()
			router.ServeHTTP(result, req)
			if result.Code != tc.want {
				t.Fatalf("status %d, want %d", result.Code, tc.want)
			}
		})
	}
}

func TestConfiguredPublicOrigin(t *testing.T) {
	t.Setenv("TRACKER_PUBLIC_ORIGIN", "https://demo-example.trycloudflare.com")
	t.Setenv("SERVER_PORT", "8081")
	for _, tc := range []struct {
		origin string
		want   bool
	}{
		{"https://demo-example.trycloudflare.com", true},
		{"https://another.trycloudflare.com", false},
		{"http://demo-example.trycloudflare.com", false},
		{"https://demo-example.trycloudflare.com.evil.test", false},
		{"https://evil@demo-example.trycloudflare.com", false},
		{"https://demo-example.trycloudflare.com/path", false},
		{"http://localhost:3001", true},
		{"http://127.0.0.1:8081", true},
		{"http://localhost:9999", false},
	} {
		if got := allowedOrigin(tc.origin, "http://127.0.0.1:3001/api/auth/me"); got != tc.want {
			t.Errorf("origin %s = %t, want %t", tc.origin, got, tc.want)
		}
	}
	t.Setenv("TRACKER_PUBLIC_ORIGIN", "")
	if allowedOrigin("https://demo-example.trycloudflare.com", "http://127.0.0.1:3001/api/auth/me") {
		t.Fatal("public access must be opt-in")
	}
}
