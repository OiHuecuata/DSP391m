import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    const backendUrl = (process.env.BACKEND_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
    return [{
      source: "/api/v1/:path*",
      destination: `${backendUrl}/api/v1/:path*`,
    }, {
      source: "/api/v2/:path*",
      destination: `${backendUrl}/api/v2/:path*`,
    }];
  },
};

export default nextConfig;
