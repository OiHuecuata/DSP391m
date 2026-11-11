import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  distDir: process.env.TRACKER_NEXT_DIST_DIR ?? ".next",
  async headers() {
    return [{source:"/:path*",headers:[{key:"X-Content-Type-Options",value:"nosniff"},{key:"X-Frame-Options",value:"DENY"},{key:"Referrer-Policy",value:"strict-origin-when-cross-origin"}]}];
  },
  async rewrites() {
    const backendUrl = (process.env.BACKEND_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
    return [{
      source: "/api/v1/:path*",
      destination: `${backendUrl}/api/v1/:path*`,
    }];
  },
};

export default nextConfig;
